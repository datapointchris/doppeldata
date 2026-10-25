#!/bin/sh
# Fails when a values hash the determinism contract test pins differs from
# the last release's while its pinned stream version is the same. A released
# stream version's values never change, so a new hash needs a raised version.
# A pin the release did not hold yet has nothing to compare.
# Needs the release tags, so the checkout fetches the full history.
# Usage: crates/check-stream-version-raised.sh
set -eu

cd "$(dirname "$0")/.."
test_file=crates/core/tests/determinism_contract.rs
profile_fixture=crates/core/tests/every-column-kind.profile.yaml
scenario_fixture=crates/core/tests/every-column-kind.scenario.yaml

pinned() {
  sed -nE "s/^const $1: [^=]+ = \"?([0-9a-f]+)\"?;$/\1/p"
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
compare() {
  name=$1
  shift
  released_values=$(printf '%s\n' "$released" | pinned "$name")
  if [ -z "$released_values" ]; then
    echo "check-stream-version-raised: $tag pins no $name, so it has nothing to compare"
    return 0
  fi
  if ! git diff --quiet "$tag" -- "$@"; then
    echo "check-stream-version-raised: a fixture of $name changed since $tag, so its hash cannot be compared"
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
