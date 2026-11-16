# Slide deck

Edit [`slides.md`](slides.md) and the local theme under `theme/`. The theme is intentionally small and uses Slidev's default layouts rather than bringing in the sibling template's custom components and demo assets.

## Palette provenance

The colors are sourced from `src/css/custom.css` in [MAAP-Project/maap-website](https://github.com/MAAP-Project/maap-website/blob/main/src/css/custom.css), retrieved from its default branch on 2026-11-16. The theme uses the site's deep-space `#0b1120` and `#152347`, primary blue `#00549f`, interactive blue `#0098db`, blue tint `#a3d9f1`, green `#22c55e`, and amber `#f59e0b`. Ropa Sans follows the website's declared brand font. These are palette and typography references, not an official MAAP design-system package.

## Template attribution

The user-preferred [Development Seed Slidev template](https://github.com/developmentseed/ds-slidev-template) was inspected at local reference commit `ba87b3b882c19df868e3b834c13be22f7bb05351`. Its use of a local Slidev theme entry point and CSS custom properties informed the structure here. No external-repository runtime dependency or DevSeed-branded component, font, image, or styling was included.

## Build

From the repository root, run `pnpm --dir slides install --frozen-lockfile` and `pnpm --dir slides build`. For a site build with the deck in the same output directory, use `./scripts/build-docs-and-slides.sh`; it builds Slidev into the ignored `docs/presentation/` directory first, then MkDocs copies those files to `build/presentation/`. Relative asset URLs keep the nested presentation working at local and GitHub Pages paths.
