# ESDIS Tech Spotlight: HLS STAC GeoParquet Archive

November 17, 2026 · Henry Rodman, Development Seed · MAAP Project

## Edit and build

From the repository root, use Node.js 22 and pnpm 9.15.0:

```bash
corepack enable
corepack prepare pnpm@9.15.0 --activate
pnpm install --frozen-lockfile
pnpm dev
```

Edit `slides.md` and `theme/`. Preview at <http://localhost:3030/>; Slidev's built-in toggle switches light/dark mode. Speaker notes contain presentation guidance. Allow roughly 37–40 minutes plus discussion, and rehearse the click reveals.

```bash
pnpm build
uv run python scripts/check-slide-site.py --browser google-chrome
python -m http.server --directory build 8000
```

The default build uses saved notebook outputs (`mkdocs.offline.yml`), without cloud queries. Open <http://localhost:8000/> and choose **Slides** in the MkDocs navigation. For GitHub Pages:

```bash
SITE_PREFIX=/hls-stac-geoparquet-archive pnpm build
uv run python scripts/check-slide-site.py --prefix /hls-stac-geoparquet-archive --browser google-chrome
```

Rebuild without `SITE_PREFIX` for localhost. The scheduled/manual Pages deployment additionally sets `MKDOCS_CONFIG=mkdocs.yml` to execute the cloud-backed notebook; see the root README for its access requirements.

- `slides.md`, `theme/`, and `public/` are authoring inputs to commit.
- `.cache/`, generated HTML, `assets/`, `images/`, `examples/`, and `build/` are ignored outputs. Put source images in `public/images/`, never `assets/`.
- The build stages Slidev output in `.cache/slidev/`, copies it alongside the source, then builds MkDocs. MkDocs excludes the authoring files and copies the generated deck.
- `pnpm dev` supports hot reload. Plain MkDocs commands do not rebuild the deck; run `pnpm build:slides` first.

Published deck: <https://maap-project.github.io/hls-stac-geoparquet-archive/presentations/esdis-tech-spotlight-2026-11-17/>. The closing QR code encodes this URL, not a local preview.

## Example context

The archive contains granule metadata and raster links, not imagery. Exact metadata objects can be read anonymously; S3 globs need bucket-listing credentials, and HLS imagery retains its Earthdata access requirements. This is an unofficial metadata copy, not a live STAC catalog.

The publisher rebuilds static Iceberg metadata over the existing monthly Parquet files. It does not maintain incremental snapshot history or guarantee snapshot isolation during replacement of those files. Proposed notification-driven ingestion and icestac integration are future work, not deployed capabilities. “Lightening the Load on CMR” is a design goal, not a measured traffic reduction.

The metadata comparison uses `HLS.L30.T14TQT.2024001T170441.v2.0`, inspected on November 16, 2026:

- [CMR converted STAC](https://cmr.earthdata.nasa.gov/search/granules.stac?concept_id=G2830065733-LPCLOUD&collection_concept_id=C2021957657-LPCLOUD).
- [CMR STAC search](https://cmr.earthdata.nasa.gov/stac/LPCLOUD/search?collections=HLSL30.v2.0&ids=HLS.L30.T14TQT.2024001T170441.v2.0&limit=1).
- [Producer STAC item](https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-public/HLSL30.020/HLS.L30.T14TQT.2024001T170441.v2.0/HLS.L30.T14TQT.2024001T170441.v2.0_stac.json).

These excerpts illustrate this granule, not every HLS record. The archive adds a collection field and stores timestamps at millisecond precision, so conversion is not byte-for-byte lossless. Footprint coordinates are geographic; raster `proj:epsg` is a separate CRS. SQL bbox overlap is not exact polygon intersection.

The rustac query returned three item dictionaries from the displayed public HTTPS object. Both January–June DuckDB count queries returned 5,101 items with anonymous credentials on November 16, 2026. Results are captured examples, not live slide execution or a performance benchmark. Iceberg simplifies file discovery, not necessarily query time.

`public/examples/hls-array.html` is a saved lazycogs/Xarray representation copied from Henry Rodman's local `lazycogs` FOSS4G NA 2026 deck; `xarray.css` adapts its display in a sandboxed iframe. It came from a prepared local Parquet index, not the slide's credentialed MAAP S3-glob adaptation, which was not rerun. Approximately 15 TB is the logical view size, not downloaded pixels. The teaser API is pseudocode. Neither example demonstrates a computed image or biomass result; masking, gaps, overlaps, compositing, and resampling need explicit policies.

## Assets and attribution

Images illustrate the narrative, not archive coverage or generated science results. Credits are also visible on the slides. Attribution does not grant a new license for third-party images or logos.

| Asset in `public/images/` | Source |
| --- | --- |
| `maap-logo.png` | [NASA MAAP website logo](https://maap-project.org/wp-content/uploads/2025/04/cropped-nasamaaplogo3.png), retrieved November 16, 2026; not a joint NASA/ESA lockup |
| `hls-constellations.png` | [NASA HLS](https://hls.gsfc.nasa.gov/wp-content/uploads/2026/03/Landsat-and-Sentinel-2-satellites-included-in-HLS-1.png), retrieved November 16, 2026 |
| `boreal-forest-biomass-density.png` | [ESA boreal forest aboveground biomass density](https://www.esa.int/ESA_Multimedia/Images/2021/10/Boreal_Forest_Above_Ground_Biomass_Density) |
| `lena-delta.jpg` | [Development Seed template asset](https://github.com/developmentseed/ds-slidev-template/blob/ba87b3b882c19df868e3b834c13be22f7bb05351/public/images/theme/lena-delta.jpg); original photographer/license not established by that template |
| `landsat8-ord-river-australia.jpg` | [USGS on Unsplash: Ord River](https://unsplash.com/photos/2BThgnOYoIc) |
| `landsat9-bangladesh-coast.jpg` | [USGS on Unsplash: Bangladesh coast](https://unsplash.com/photos/eGGENWtikd0) |
| `landsat9-apostle-islands-lake-superior.jpg` | [USGS on Unsplash: Apostle Islands](https://unsplash.com/photos/j7HqdQqn7Jo) |
| `landsat9-kangerdlugssuaq-greenland.jpg` | [USGS on Unsplash: Kangerdlugssuaq Glacier](https://unsplash.com/photos/PgL1p8TBGNQ) |
| `slides-qr.svg` | Generated locally with Python `qrcode`, medium error correction and four-module quiet zone; white background for both themes |

The Landsat images were sourced from the Development Seed template at commit `ba87b3b882c19df868e3b834c13be22f7bb05351` and resized to a maximum dimension of 1,920 pixels at JPEG quality 85 (about 2.7 MB combined instead of 16.6 MB). Lena Delta remains unchanged.

The local theme and opening layout adapt the [Development Seed Slidev template](https://github.com/developmentseed/ds-slidev-template) at that same commit, without its branding or runtime components. Colors and Ropa Sans follow [MAAP website CSS](https://github.com/MAAP-Project/maap-website/blob/main/src/css/custom.css), inspected November 16, 2026; this is not an official design-system package. Fonts load from Google Fonts, with CSS fallbacks.

The original JSON-to-table illustrations reference [Pete Gadomski's cloud-native metadata presentation](https://www.gadom.ski/presentations/2025-11-04-stac-geoparquet.html#/4/2); no presentation images were copied. The Iceberg diagram follows the [official specification](https://iceberg.apache.org/spec/#overview) and [PyIceberg Add Files](https://py.iceberg.apache.org/api/#add-files).
