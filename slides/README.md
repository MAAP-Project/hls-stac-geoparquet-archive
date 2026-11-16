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

`slides.md` and `theme/` are committed source. Only generated files are ignored. `pnpm build:slides` builds just the deck into `docs/presentations/esdis-tech-spotlight-2026-11-17/`; the combined build then lets MkDocs copy it into the same path under `build/`. The site endpoint is `presentations/esdis-tech-spotlight-2026-11-17/`. Relative asset URLs support both localhost and GitHub Pages without a separate build configuration.

For a site preview, run `python -m http.server --directory build 8000`. For MkDocs live preview, run `pnpm build:slides` followed by `uv run mkdocs serve --config-file mkdocs.offline.yml`; slide edits need another `pnpm build:slides`. Use `pnpm dev` instead for Slidev hot reload. See the root README for cloud-backed notebook execution.
