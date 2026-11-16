# Slide deck

Edit [`slides.md`](slides.md) and the local theme under `theme/`. The theme is intentionally small and uses Slidev's default layouts rather than bringing in the sibling template's custom components and demo assets.

## Palette provenance

The colors are sourced from `src/css/custom.css` in [MAAP-Project/maap-website](https://github.com/MAAP-Project/maap-website/blob/main/src/css/custom.css), retrieved from its default branch on 2026-11-16. The theme uses the site's deep-space `#0b1120` and `#152347`, primary blue `#00549f`, interactive blue `#0098db`, blue tint `#a3d9f1`, green `#22c55e`, and amber `#f59e0b`. Ropa Sans follows the website's declared brand font. These are palette and typography references, not an official MAAP design-system package.

## Template attribution

The user-preferred [Development Seed Slidev template](https://github.com/developmentseed/ds-slidev-template) was inspected at local reference commit `ba87b3b882c19df868e3b834c13be22f7bb05351`. Its use of a local Slidev theme entry point and CSS custom properties informed the structure here. No external-repository runtime dependency or DevSeed-branded component, font, image, or styling was included.

## Build

Run these commands from the repository root (Node.js 22, pnpm 9.15.0):

```bash
pnpm install --frozen-lockfile
pnpm dev          # Live editing at http://localhost:3030/
pnpm build        # Build the deck and offline MkDocs site
```

All deck materials live in `docs/presentations/esdis-tech-spotlight-2026-11-17/`:

```text
slides.md       # Tracked Slidev source
README.md       # Tracked authoring notes
theme/          # Tracked local MAAP theme
index.html      # Generated, ignored
404.html        # Generated, ignored
_redirects      # Generated, ignored
assets/         # Generated, ignored; put source images in public/ instead
```

`pnpm build:slides` stages output in `.cache/slidev/esdis-tech-spotlight-2026-11-17/`, then copies generated files alongside the source without deleting Markdown or theme files. MkDocs excludes `slides.md`, this README, and `theme/` from its site build and copies the generated deck to the matching path under `build/`. The site endpoint is `presentations/esdis-tech-spotlight-2026-11-17/`. Slidev's router requires an explicit mount path. The default build uses `/presentations/esdis-tech-spotlight-2026-11-17/`. For GitHub Pages, set `SITE_PREFIX=/hls-stac-geoparquet-archive` when running `pnpm build` or `pnpm build:slides`. Rebuild without that variable for localhost; a Pages build is not interchangeable with a root-mounted local preview.

For a site preview, run `python -m http.server --directory build 8000`. For MkDocs live preview, run `pnpm build:slides` followed by `uv run mkdocs serve --config-file mkdocs.offline.yml`; slide edits need another `pnpm build:slides`. Use `pnpm dev` instead for Slidev hot reload. See the root README for cloud-backed notebook execution.
