#!/usr/bin/env bash
set -euo pipefail

config="${MKDOCS_CONFIG:-mkdocs.offline.yml}"

rm -rf docs/presentation
pnpm --dir slides exec slidev build --out ../docs/presentation --base ./
uv run mkdocs build --config-file "$config"
