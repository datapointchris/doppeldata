#!/bin/sh
# Fails when the determinism contract test's pinned values hash differs from
# the last release's while its pinned stream version is the same. A released
# stream version's values never change, so a new hash needs a raised version.
# Needs the release tags, so the checkout fetches the full history.
# Usage: crates/check-stream-version-raised.sh
set -eu

cd "$(dirname "$0")/.."
test_file=crates/core/tests/determinism_contract.rs
fixture=crates/core/tests/every-column-kind.profile.yaml

pinned() {
  sed -nE "s/^const $1: [^=]+ = \"?([0-9a-f]+)\"?;$/\1/p"
}

if ! tag=$(git describe --tags --abbrev=0 --match 'v*' 2>/dev/null); then
  echo "check-stream-version-raised: no release tag, so no released values to compare"
  exit 0
fi

released=$(git show "$tag:$test_file" 2>/dev/null || true)
released_version=$(printf '%s\n' "$released" | pinned PINNED_STREAM_VERSION)
released_values=$(printf '%s\n' "$released" | pinned PINNED_VALUES)
if [ -z "$released_version" ] || [ -z "$released_values" ]; then
  echo "check-stream-version-raised: $tag pins no values"
  exit 0
fi

if ! git diff --quiet "$tag" -- "$fixture"; then
  echo "check-stream-version-raised: the fixture changed since $tag, so its hash cannot be compared"
  exit 0
fi

version=$(pinned PINNED_STREAM_VERSION <"$test_file")
values=$(pinned PINNED_VALUES <"$test_file")
if [ -z "$version" ] || [ -z "$values" ]; then
  echo "check-stream-version-raised: no PINNED_STREAM_VERSION and PINNED_VALUES in $test_file" >&2
  exit 1
fi

if [ "$version" = "$released_version" ] && [ "$values" != "$released_values" ]; then
  echo "check-stream-version-raised: the pinned values changed since $tag at stream version $version." >&2
  echo "Raise STREAM_VERSION and PINNED_STREAM_VERSION with them." >&2
  exit 1
fi
echo "check-stream-version-raised: stream version $version against $tag's $released_version, values consistent"
