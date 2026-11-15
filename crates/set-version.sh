#!/bin/sh
# Writes a release's version into the workspace manifest, which carries 0.0.0 in
# git, so the wheel, the sdist and the engine's VERSION all name the tag.
# cargo set-version parses the manifest and sets [workspace.package] version,
# which every member inherits.
# Usage: crates/set-version.sh 1.2.3
set -eu

version=$1
manifest=$(dirname "$0")/Cargo.toml

# A release build installs every tool it runs at a pinned version, into a
# directory of its own so nothing on the runner's PATH is replaced.
cargo_edit_version=0.13.0
tools=$(mktemp -d)
trap 'rm -rf "$tools"' EXIT
cargo install --quiet --locked --version "$cargo_edit_version" --root "$tools" cargo-edit

PATH="$tools/bin:$PATH" cargo set-version --workspace --manifest-path "$manifest" "$version"
