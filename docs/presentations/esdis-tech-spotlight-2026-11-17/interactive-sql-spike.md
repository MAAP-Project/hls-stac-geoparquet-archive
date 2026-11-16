# Interactive SQL in the Slidev deck: research spike

**Status: Research complete. The browser-side parquet path is unverified; the Iceberg-backed browser path is unsupported by the current official WASM build configuration and absent from the tested extension catalog URL.** This is a research report, not a UI implementation.

## Bottom line

A genuine “edit SQL, click Run, see rows” example is feasible inside a Slidev Vue component. The cleanest small demo is DuckDB-Wasm reading the one public January 2024 parquet object by its exact HTTPS URL, with a small result limit and no bucket listing. That is *not* an Iceberg query. The current official DuckDB-Wasm build configuration omits Iceberg, and the DuckDB 1.5.5 `wasm_eh` extension catalog URL tested below returns 404. Do not label a WASM example `iceberg_scan` unless a different/custom build is proven to support it.

The exact parquet is 24,927,541 bytes. A 25 MB file is not necessarily fast merely because it is columnar; choose projected columns, filter first, limit returned rows, show a loading state, and test on the actual presentation network. If the live browser request fails, use the exact-path remote-backend fallback described below or do not promise live execution at the talk.

## What I checked

### Data and query, actually executed

The native data query was executed while this checkout was at `29dbe122bacfc3a4551cfb2ea660a5819f1ecc83` (the assigned base), using `uv run python` and native DuckDB 1.5.5:

- `HEAD` for `.../HLSL30_2.0/year=2024/month=1/HLSL30_2.0-2024-1.parquet`: **200**, `Accept-Ranges: bytes`, `Content-Length: 24927541`.
- `HEAD` for `.../HLSL30_2.0/iceberg/metadata/latest.metadata.json`: **200**, length 23,471 bytes.
- Native DuckDB `read_parquet(exact_https_href)` succeeded. `count(*)` returned **102,259**. The schema has `geometry` as `GEOMETRY('OGC:CRS84')` and `bbox` as a struct `{xmin,ymin,xmax,ymax}`.
- After `INSTALL spatial; LOAD spatial`, `count(*)` for `ST_Intersects(geometry, ST_MakeEnvelope(-100,40,-90,50))` returned **550**. This is a native DuckDB run, **not DuckDB-Wasm**.
- This direct query uses the one exact object URL. It does not depend on wildcard expansion or `ListBucket`.

A browser-like raw range GET with `Origin: https://example.org` and `Range: bytes=0-31` returned **206**, `Content-Range: bytes 0-31/24927541`, and `Access-Control-Allow-Origin: *`. An OPTIONS preflight with that origin, requested method GET, and requested header `range` returned **403 `AccessForbidden` / `CORSResponse: This CORS request is not allowed**. A single `Range` header is CORS-safelisted in modern Fetch, so that synthetic preflight does not establish that DuckDB-Wasm's simple range GET will fail; the range GET's ACAO header is positive evidence. No actual browser fetch from the deployed Slidev origin was run. Thus browser parquet access remains **unverified**, not proven broken or working.

### Iceberg / WASM capability

The current deck and project README show native DuckDB's intended Iceberg usage: `INSTALL iceberg; LOAD iceberg; ... iceberg_scan('s3://.../iceberg/metadata/latest.metadata.json')`. The README explicitly describes metadata as avoiding `ListBucket`; its S3 credential setup is for authenticated/native use. See [project README](../../../README.md#querying-the-archive) and the [deck's SQL slide](slides.md).

Primary evidence is stronger than the previous report's absence-of-search-results argument. DuckDB-Wasm's official [`extension_config_wasm.cmake`](https://github.com/duckdb/duckdb-wasm/blob/main/extension_config_wasm.cmake) explicitly loads `json`, `parquet`, `autocomplete`, `icu`, `tpcds`, `tpch`, and statically linked `nanoarrow`; it does **not** load `iceberg`. The official Iceberg extension docs describe its native distribution and `iceberg_scan`: <https://duckdb.org/docs/stable/core_extensions/iceberg/overview.html>. The official WASM overview and extension docs describe the distinct WASM builds/catalog: <https://duckdb.org/docs/stable/clients/wasm/overview.html> and <https://duckdb.org/docs/stable/clients/wasm/extensions.html>. As a concrete catalog check, `curl -I https://extensions.duckdb.org/v1.5.5/wasm_eh/iceberg.duckdb_extension.gz` returned **HTTP 404** on 2026-11-16. This is the catalog path for DuckDB 1.5.5's base WASM `wasm_eh` platform; it is direct HTTP evidence for absence of that prebuilt extension, not a browser SQL test. I did not have `@duckdb/duckdb-wasm` in the supplied sibling `node_modules` (only Slidev/Vue tooling), so no browser `INSTALL/LOAD iceberg` SQL was run. Therefore:

- **Native Iceberg scan via latest metadata JSON:** supported by the project's existing documented workflow; exact metadata HEAD is public.
- **Iceberg from DuckDB-Wasm:** **unsupported by the current official build/catalog evidence tested**: absent from the WASM CMake extension load list, and its versioned `wasm_eh` extension URL is 404 for DuckDB 1.5.5. `INSTALL iceberg`/`LOAD iceberg` was not executed in a browser. A custom WASM build could in principle change support, so this is not a claim about every possible custom build.
- **Anonymous no-list behavior for Iceberg:** the project deliberately publishes exact file paths in static metadata. Still, whether the selected runtime can follow every metadata/manifest/data href anonymously, and whether each HTTP request has browser CORS access, is a separate question. A successful metadata HEAD alone does not prove the complete scan.

## Development Seed examples to follow

These are concrete source examples, not generic recommendations. The GitHub search query `DuckDB-Wasm org:developmentseed` surfaced them; repository files below were read from their remote GitHub default/referenced revisions.

1. **[`developmentseed/stac-map`](https://github.com/developmentseed/stac-map)**, current package manifest version 2.4.1, pins `@duckdb/duckdb-wasm` and `duckdb-wasm-kit` in [`package.json`](https://github.com/developmentseed/stac-map/blob/28298db892e9372daea7ff0c6ef9322ae3f22ca3/package.json). [`src/utils/stac-geoparquet.ts`](https://github.com/developmentseed/stac-map/blob/28298db892e9372daea7ff0c6ef9322ae3f22ca3/src/utils/stac-geoparquet.ts) executes SQL against `read_parquet(href, hive_partitioning=...)` through an `AsyncDuckDBConnection`, returns Arrow tables, applies filters, and converts WKB geometry. [`src/hooks/stac.ts`](https://github.com/developmentseed/stac-map/blob/28298db892e9372daea7ff0c6ef9322ae3f22ca3/src/hooks/stac.ts) wraps those queries in query/cache state; [`src/components/stac-geoparquet.tsx`](https://github.com/developmentseed/stac-map/blob/28298db892e9372daea7ff0c6ef9322ae3f22ca3/src/components/stac-geoparquet.tsx) provides the component boundary and error handling. This is the closest match for browser-side GeoParquet + DuckDB-Wasm, but it is React, not a Slidev/Vue drop-in and not an arbitrary SQL editor.
2. **[`developmentseed/spatial-access-measures`](https://github.com/developmentseed/spatial-access-measures)** [`src/components/db.tsx`](https://github.com/developmentseed/spatial-access-measures/blob/ce8d2d896fbb75303e076a786d04bce1a247afb6/src/components/db.tsx) starts DuckDB-Wasm, registers an exact remote parquet URL as an HTTP file, connects, and loads `spatial`. [`src/App.tsx`](https://github.com/developmentseed/spatial-access-measures/blob/ce8d2d896fbb75303e076a786d04bce1a247afb6/src/App.tsx) runs SQL through `useDuckDbQuery` and renders loading state. This is a clear example of exact-object remote access and browser SQL integration; again React.
3. **[`developmentseed/sat-in-browser-example-app`](https://github.com/developmentseed/sat-in-browser-example-app)** README describes a real browser pipeline: DuckDB-Wasm + remote chunk reads + SQL analysis + deck.gl visualization, with explicit range reads and costs. It demonstrates that client-side query/visualization is practical but is an involved data app, not a tiny presentation embed: [`README.md`](https://github.com/developmentseed/sat-in-browser-example-app/blob/c1f09c37a5cebdf1e06a2f53ac5d23bf1e2063e9/README.md).

The useful pattern to borrow is one initialized async database connection owned by a Vue component, one explicit query-on-submit action, an Arrow/result-table view, and visible loading/error states. Do not pull in their React app or a whole mapping stack for this deck.

## Integration options

### Preferred spike if browser check passes: Vue + DuckDB-Wasm + exact parquet URL

Add a local Vue SFC under this deck and import it in `slides.md` using Slidev's Vue component mechanism. Initialize DuckDB-Wasm once for the mounted component, bind an editable SQL textarea, execute only on Run, display column headers/rows plus timing/error, and dispose the connection/worker on unmount. Keep the initial example prefilled with a read-only query against the exact `.parquet` HTTPS URL; no `**`, glob, prefix discovery, or S3 credentials. Filter spatially using stored bbox fields if possible (cheap and robust), or verify `spatial` extension availability in WASM before showing a geometry function. Explain that bbox overlap is a bounding-box predicate, not exact geometry intersection.

This is genuinely interactive SQL, but anonymous browser reads must first be proven from the presentation's actual origin. A public object URL and native DuckDB success do not prove that browser fetch works. Avoid arbitrary SQL if a future endpoint is introduced; the direct browser case is public read-only data, but queries can still cause broad downloads, long CPU use, or browser memory pressure.

### Iceberg option: server-side query endpoint, only if the demo must show Iceberg

A minimal backend can run native DuckDB with the existing Iceberg extension and query the static latest metadata JSON. Browser submits a constrained query or a fixed query template and gets bounded JSON/Arrow results. Configure outbound access for exact metadata/manifest/data object GETs only; never grant or rely on bucket listing. Prefer a finite parameterized query API over accepting arbitrary SQL from unauthenticated users. This adds an application to deploy, operate, secure, and keep available, and the demo now depends on network/server uptime.

A local native DuckDB process is another reliable presenter-only fallback but is not a live browser slide. For a talk, static output or precomputed result remains the lowest-risk fallback if live service/browser networking is uncertain.

### Static example fallback

Keep SQL editable as displayed code and reveal a recorded/precomputed result table or image. Clearly label the result as precomputed, not Run-button execution. This is less interactive but deterministic and preserves the lesson about exact object paths and Iceberg metadata.

## Recommendation and next bounded step

Do **not** promise browser Iceberg. The bounded follow-up is a single browser probe from the actual Slidev origin that initializes DuckDB-Wasm and reads the exact January parquet object, recording network requests and CORS headers. Then check whether `spatial` is in that WASM build and try the bbox query. Since the tested Iceberg wasm_eh catalog artifact is absent, only revisit Iceberg after a custom-build investigation; otherwise choose a controlled remote native-DuckDB endpoint or a static result for the talk.

Reliability costs: WASM bundle/worker startup, first-load download and browser memory, public bucket CORS policy, attendee Wi-Fi, extension downloads, and presentation-origin differences. Security costs: arbitrary SQL can be computationally abusive if proxied; never put AWS credentials in the browser; server-side option requires strict query limits and read-only access. This spike made no runtime/dependency changes and performed no browser execution.

## Sources and caveats

- [DuckDB-Wasm overview](https://duckdb.org/docs/stable/clients/wasm/overview.html), [WASM extensions](https://duckdb.org/docs/stable/clients/wasm/extensions.html), [Iceberg extension](https://duckdb.org/docs/stable/core_extensions/iceberg/overview.html).
- [Project README](../../../README.md), especially public exact-path layout and existing Iceberg SQL; [`slides.md`](slides.md).
- Development Seed repository/source links above. GitHub tool searches included code results not all authored by Development Seed; only the three repositories explicitly under the `developmentseed` org are cited as examples.
- HTTP checks and native DuckDB execution were performed during this spike. Official DuckDB-Wasm build configuration was read from GitHub and the DuckDB 1.5.5 wasm_eh Iceberg artifact URL returned 404. No DuckDB-Wasm browser SQL was executed, including `INSTALL`/`LOAD iceberg`; browser parquet access is unverified. CORS checks used `https://example.org`, not the deployed Slidev origin. The synthetic 403 preflight may not represent the simple Range GET used by Fetch; the observed range GET did include ACAO `*`.
