#!/bin/sh
# Writes the extension's stub, maturin's introspection output with the fixes and
# formatting the ruff hooks apply, so the committed stub is both. maturin writes
# `str |None` and leaves imports unsorted, which the hooks would otherwise
# rewrite into a stub maturin does not generate.
# Usage: crates/write-stub.sh library/src/doppeldata/_core.pyi
set -eu

case $1 in
  /*) target=$1 ;;
  *) target=$PWD/$1 ;;
esac
library=$(dirname "$0")/../library
stubs=$(mktemp -d)
trap 'rm -rf "$stubs"' EXIT

cd "$library"
uv run maturin generate-stubs --quiet --out "$stubs"
# The stdin path places the text in the library, where ruff reads the repo's config.
uv run ruff check --fix-only --quiet --stdin-filename src/doppeldata/_core.pyi <"$stubs/doppeldata/_core.pyi" \
  | uv run ruff format --quiet --stdin-filename src/doppeldata/_core.pyi >"$target"
