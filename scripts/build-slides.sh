#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
# Slidev's router needs the full mount path, not a relative asset base.
base="${SITE_PREFIX:-}/presentations/esdis-tech-spotlight-2026-11-17/"
# Stage generated files separately so a clean build cannot erase deck source.
rm -rf .cache/slidev/esdis-tech-spotlight-2026-11-17
corepack pnpm exec slidev build \
  docs/presentations/esdis-tech-spotlight-2026-11-17/slides.md \
  --out "$PWD/.cache/slidev/esdis-tech-spotlight-2026-11-17" --base "$base"
rm -rf docs/presentations/esdis-tech-spotlight-2026-11-17/assets
cp -R .cache/slidev/esdis-tech-spotlight-2026-11-17/. \
  docs/presentations/esdis-tech-spotlight-2026-11-17/
