#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
config="${MKDOCS_CONFIG:-mkdocs.offline.yml}"

rm -rf docs/presentations/esdis-tech-spotlight-2026-11-17
corepack pnpm build:slides
uv run mkdocs build --config-file "$config"
