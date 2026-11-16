# Slide deck

Edit [`slides.md`](slides.md) and the local theme under `theme/`. The theme is intentionally small and uses Slidev's default layouts rather than bringing in the sibling template's custom components and demo assets.

## Palette provenance

The colors are sourced from `src/css/custom.css` in [MAAP-Project/maap-website](https://github.com/MAAP-Project/maap-website/blob/main/src/css/custom.css), retrieved from its default branch on 2026-11-16. The theme uses the site's deep-space `#0b1120` and `#152347`, primary blue `#00549f`, interactive blue `#0098db`, blue tint `#a3d9f1`, green `#22c55e`, and amber `#f59e0b`. Ropa Sans follows the website's declared brand font. These are palette and typography references, not an official MAAP design-system package.

## Light and dark mode

The theme follows Slidev's built-in light/dark toggle (`html.dark`). Semantic variables in `theme/styles.css` control text, headings, links, muted notes, and card backgrounds in both modes. Slide titles use 2.5rem text with space below, section headings use 1.75rem, and the cover title uses 3.5rem. The cover keeps its dark MAAP gradient in either mode. Use `text-muted` for secondary text rather than a fixed gray utility.

`theme/setup/shiki.ts` selects Shiki's GitHub light/dark palettes; code backgrounds match those palettes. Inline code uses the theme accent without overriding highlighted code tokens. When changing colors, check both modes, including the pipeline and SQL slides.

## Template attribution

The user-preferred [Development Seed Slidev template](https://github.com/developmentseed/ds-slidev-template) was inspected at local reference commit `ba87b3b882c19df868e3b834c13be22f7bb05351`. Its use of a local Slidev theme entry point and CSS custom properties informed the structure here. No external-repository runtime dependency or DevSeed-branded component, font, image, or styling was included.

## Verified anonymous SQL comparison

On 2026-11-16, DuckDB 1.5.5 returned **550** HLSL30 items from both the exact January 2024 Parquet object and the static Iceberg metadata href. Both queries used `datetime >= 2024-01-01T00:00:00Z AND datetime < 2024-02-01T00:00:00Z` and bbox-envelope overlap with `(-100, 40, -90, 50)`. This is an envelope filter, not an exact geometry intersection. Anonymous `HEAD` requests to both public S3 objects returned HTTP 200.

Reproduce from the repository root with the project's dev dependency group. This creates a new DuckDB connection for each source and explicitly uses empty S3 credentials rather than a credential chain:

```bash
env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN \
  -u AWS_PROFILE -u AWS_DEFAULT_PROFILE uv run --group dev python - <<'PY'
import logging

import duckdb

logging.basicConfig(level=logging.INFO, format="%(message)s")

parquet = (
    "s3://nasa-maap-data-store/file-staging/nasa-map/"
    "hls-stac-geoparquet-archive/v2/HLSL30_2.0/"
    "year=2024/month=1/HLSL30_2.0-2024-1.parquet"
)
metadata = (
    "s3://nasa-maap-data-store/file-staging/nasa-map/"
    "hls-stac-geoparquet-archive/v2/HLSL30_2.0/"
    "iceberg/metadata/latest.metadata.json"
)
where = """
datetime >= TIMESTAMPTZ '2024-01-01 00:00:00+00'
AND datetime < TIMESTAMPTZ '2024-02-01 00:00:00+00'
AND bbox.xmin <= -90 AND bbox.xmax >= -100
AND bbox.ymin <= 50 AND bbox.ymax >= 40
"""

for label, source, extensions in [
    ("direct parquet", f"read_parquet('{parquet}')", "INSTALL httpfs; LOAD httpfs;"),
    (
        "static iceberg",
        f"iceberg_scan('{metadata}')",
        "INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;",
    ),
]:
    connection = duckdb.connect()
    connection.execute(extensions)
    connection.execute(
        "CREATE SECRET anon_s3 (TYPE S3, PROVIDER CONFIG, "
        "KEY_ID '', SECRET '', REGION 'us-west-2')"
    )
    count = connection.execute(
        f"SELECT count(*) FROM {source} WHERE {where}"
    ).fetchone()[0]
    logging.info("%s: %s", label, count)
    connection.close()
PY
```

Expected output:

```text
direct parquet: 550
static iceberg: 550
```

The monthly Parquet href pins the direct scan to one known object, so it needs no bucket listing. The Iceberg scan starts from only the published `latest.metadata.json` href and follows its file references. Do not replace the empty-credential secret with `credential_chain` when reproducing the anonymous-access test.

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
