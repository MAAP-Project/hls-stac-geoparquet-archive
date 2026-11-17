#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
config="${MKDOCS_CONFIG:-mkdocs.offline.yml}"

corepack pnpm build:slides
uv run mkdocs build --config-file "$config"
