#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
# Slidev's router needs the full mount path, not a relative asset base.
base="${SITE_PREFIX:-}/presentations/esdis-tech-spotlight-2026-11-17/"
rm -rf docs/presentations/esdis-tech-spotlight-2026-11-17
corepack pnpm exec slidev build slides/slides.md \
  --out ../docs/presentations/esdis-tech-spotlight-2026-11-17 --base "$base"
