#!/bin/sh
# Fails when a values hash the determinism contract test pins, or the hash of
# the hints library/tests/test_typed_column_hints.py pins, differs from the
# last release's while its pinned stream version is the same. A released
# stream version's values never change, so a new hash needs a raised version.
# A pin the release did not hold yet has nothing to compare. Where a pin's
# fixture changed since the release at the same stream version and its hash is
# still the release's, the determinism contract test holds this build to that
# hash for the fixture as it now reads, so the change moved no value. Where the
# hash moved too, this build generates the release's own fixtures and must
# reach the release's hash, which needs cargo.
# Needs the release tags, so the checkout fetches the full history.
# Usage: crates/check-stream-version-raised.sh
set -eu

cd "$(dirname "$0")/.."
test_file=crates/core/tests/determinism_contract.rs
profile_fixture=crates/core/tests/every-column-kind.profile.yaml
scenario_fixture=crates/core/tests/every-column-kind.scenario.yaml
cdc_fixture=crates/core/tests/every-column-kind.cdc.scenario.yaml
bundle_fixture=crates/core/tests/every-column-kind.bundle.yaml

pinned() {
  sed -nE "s/^const $1: [^=]+ = \"?([0-9a-f]+)\"?;$/\1/p"
}

number() {
  sed -nE "s/^const $1: u64 = ([0-9_]+);$/\1/p" | tr -d _
}

# Generates the release's own fixtures, a profile and an optional scenario, with this build, and
# requires the hash the release pinned for them.
generate_released() {
  pin=$1
  shift
  dir=$(mktemp -d)
  trap 'rm -rf "$dir"' EXIT
  git show "$tag:$1" >"$dir/profile.yaml"
  scenario=
  if [ "$#" -gt 1 ]; then
    git show "$tag:$2" >"$dir/scenario.yaml"
    scenario=$dir/scenario.yaml
  fi
  DOPPELDATA_RELEASED_PROFILE=$dir/profile.yaml DOPPELDATA_RELEASED_SCENARIO=$scenario \
    DOPPELDATA_RELEASED_SEED=$(printf '%s\n' "$released" | number SEED) \
    DOPPELDATA_RELEASED_ROWS=$(printf '%s\n' "$released" | number ROWS) \
    DOPPELDATA_RELEASED_VALUES=$pin \
    cargo test --manifest-path crates/Cargo.toml -p doppeldata-core --test determinism_contract -- \
    --ignored --exact a_released_fixture_generates_the_values_its_release_pinned
}

if ! tag=$(git describe --tags --abbrev=0 --match 'v*' 2>/dev/null); then
  echo "check-stream-version-raised: no release tag, so no released values to compare"
  exit 0
fi

released=$(git show "$tag:$test_file" 2>/dev/null || true)
released_version=$(printf '%s\n' "$released" | pinned PINNED_STREAM_VERSION)
if [ -z "$released_version" ]; then
  echo "check-stream-version-raised: $tag pins no values"
  exit 0
fi

version=$(pinned PINNED_STREAM_VERSION <"$test_file")
if [ -z "$version" ]; then
  echo "check-stream-version-raised: no PINNED_STREAM_VERSION in $test_file" >&2
  exit 1
fi

# Each pin, and the fixtures its values are generated from.
compared=
compare() {
  name=$1
  shift
  compared="$compared $name "
  released_values=$(printf '%s\n' "$released" | pinned "$name")
  if [ -z "$released_values" ]; then
    echo "check-stream-version-raised: $tag pins no $name, so it has nothing to compare"
    return 0
  fi
  if ! git diff --quiet "$tag" -- "$@"; then
    if [ "$version" != "$released_version" ]; then
      echo "check-stream-version-raised: a fixture of $name changed since $tag, and the stream version was raised"
      return 0
    fi
    if [ "$(pinned "$name" <"$test_file")" = "$released_values" ]; then
      echo "check-stream-version-raised: a fixture of $name changed since $tag, and its pinned hash is still $tag's"
      return 0
    fi
    echo "check-stream-version-raised: a fixture of $name changed since $tag, so this build generates $tag's own"
    generate_released "$released_values" "$@"
    echo "check-stream-version-raised: $tag's fixtures generate its pinned $name with this build"
    return 0
  fi
  values=$(pinned "$name" <"$test_file")
  if [ -z "$values" ]; then
    echo "check-stream-version-raised: $tag pins $name, and $test_file no longer does" >&2
    return 1
  fi
  if [ "$version" = "$released_version" ] && [ "$values" != "$released_values" ]; then
    echo "check-stream-version-raised: $name changed since $tag at stream version $version." >&2
    echo "Raise STREAM_VERSION and PINNED_STREAM_VERSION with it." >&2
    return 1
  fi
  echo "check-stream-version-raised: $name at stream version $version against $tag's $released_version, consistent"
}

compare PINNED_VALUES "$profile_fixture"
compare PINNED_SCENARIO_VALUES "$profile_fixture" "$scenario_fixture"
compare PINNED_CDC_VALUES "$profile_fixture" "$cdc_fixture"
compare PINNED_BUNDLE_VALUES "$bundle_fixture"

sed -nE 's/^const (PINNED_[A-Z_]*VALUES): .*/\1/p' <"$test_file" | while read -r name; do
  case "$compared" in
    *" $name "*) ;;
    *)
      echo "check-stream-version-raised: $test_file pins $name, and this script names no fixture for it" >&2
      exit 1
      ;;
  esac
done

# The hints the library fills a typed column with, which no fixture the engine reads holds, so a
# Python test pins them.
hints_file=library/tests/test_typed_column_hints.py
python_pin() {
  sed -nE "s/^$1 = '([0-9a-f]+)'$/\1/p"
}
released_hints=$(git show "$tag:$hints_file" 2>/dev/null | python_pin PINNED_TYPE_HINTS)
hints=$(python_pin PINNED_TYPE_HINTS <"$hints_file")
if [ -z "$released_hints" ]; then
  echo "check-stream-version-raised: $tag pins no PINNED_TYPE_HINTS, so it has nothing to compare"
elif [ "$version" = "$released_version" ] && [ "$hints" != "$released_hints" ]; then
  echo "check-stream-version-raised: PINNED_TYPE_HINTS changed since $tag at stream version $version." >&2
  echo "Raise STREAM_VERSION and both PINNED_STREAM_VERSIONs with it." >&2
  exit 1
else
  echo "check-stream-version-raised: PINNED_TYPE_HINTS at stream version $version against $tag's $released_version, consistent"
fi
