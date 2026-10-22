#!/bin/sh
# Writes a release's version into the workspace manifest, which carries 0.0.0 in
# git, so the wheel, the sdist and the engine's VERSION all name the tag.
# Usage: crates/set-version.sh 1.2.3
set -eu

version=$1
manifest=$(dirname "$0")/Cargo.toml

# -i.orig is the in-place form GNU and BSD sed both accept.
sed -i.orig "s/^version = \"0\.0\.0\"$/version = \"$version\"/" "$manifest"
rm "$manifest.orig"

if ! grep -qx "version = \"$version\"" "$manifest"; then
  echo "set-version: no 'version = \"0.0.0\"' line in $manifest to stamp" >&2
  exit 1
fi
