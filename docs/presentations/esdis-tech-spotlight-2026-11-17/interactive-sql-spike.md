# Interactive SQL in the Slidev deck: research spike

**Status: READY** for a small browser-side parquet demo; **BLOCKED** for an Iceberg-backed browser demo until the WASM extension and browser fetch path are proven. This is a research report, not a UI implementation.

## Bottom line

A genuine “edit SQL, click Run, see rows” example is feasible inside a Slidev Vue component. The cleanest small demo is DuckDB-Wasm reading the one public January 2024 parquet object by its exact HTTPS URL, with a small result limit and no bucket listing. That is *not* an Iceberg query. The DuckDB Iceberg extension is available to native DuckDB, but I found no primary-source evidence that it is built for or loadable by DuckDB-Wasm; do not label a WASM example `iceberg_scan` until a real browser test succeeds.

The exact parquet is 24,927,541 bytes. A 25 MB file is not necessarily fast merely because it is columnar; choose projected columns, filter first, limit returned rows, show a loading state, and test on the actual presentation network. If the live browser CORS request fails (it did for an arbitrary-origin preflight; see below), use the exact-path remote-backend fallback described below or do not promise live execution at the talk.

## What I checked

### Data and query, actually executed

At the root worktree HEAD (`29dbe122bacfc3a4551cfb2ea660a5819f1ecc83`), using `uv run python` and native DuckDB 1.5.5:

- `HEAD` for `.../HLSL30_2.0/year=2024/month=1/HLSL30_2.0-2024-1.parquet`: **200**, `Accept-Ranges: bytes`, `Content-Length: 24927541`.
- `HEAD` for `.../HLSL30_2.0/iceberg/metadata/latest.metadata.json`: **200**, length 23,471 bytes.
- Native DuckDB `read_parquet(exact_https_href)` succeeded. `count(*)` returned **102,259**. The schema has `geometry` as `GEOMETRY('OGC:CRS84')` and `bbox` as a struct `{xmin,ymin,xmax,ymax}`.
- After `INSTALL spatial; LOAD spatial`, `count(*)` for `ST_Intersects(geometry, ST_MakeEnvelope(-100,40,-90,50))` returned **550**. This is a native DuckDB run, **not DuckDB-Wasm**.
- This direct query uses the one exact object URL. It does not depend on wildcard expansion or `ListBucket`.

A browser-like raw range GET with `Origin: https://example.org` and `Range: bytes=0-31` returned **206**, `Content-Range: bytes 0-31/24927541`, and `Access-Control-Allow-Origin: *`. However, an OPTIONS preflight with that origin, requested method GET, and requested header `range` returned **403 `AccessForbidden` / `CORSResponse: This CORS request is not allowed`**. The actual DuckDB-Wasm request sequence and deployed Slidev origin were not tested in a browser. Treat direct browser access as **unverified / likely CORS-sensitive**, not as established working behavior. S3 anonymous object reads do not imply browser CORS permission.

### Iceberg / WASM capability

The current deck and project README show native DuckDB's intended Iceberg usage: `INSTALL iceberg; LOAD iceberg; ... iceberg_scan('s3://.../iceberg/metadata/latest.metadata.json')`. The README explicitly describes metadata as avoiding `ListBucket`; its S3 credential setup is for authenticated/native use. See [project README](../../../README.md#querying-the-archive) and the [deck's SQL slide](slides.md).

DuckDB's Iceberg extension docs describe the native extension and `iceberg_scan`: <https://duckdb.org/docs/stable/core_extensions/iceberg/overview.html>. DuckDB-Wasm is a separately compiled browser client with an extension catalog/build support surface: <https://duckdb.org/docs/stable/clients/wasm/overview.html> and <https://duckdb.org/docs/stable/clients/wasm/extensions.html>. These sources do not establish that Iceberg is one of the currently shipped/usable WASM extensions. The remote code/reference searches did not find an affirmative WASM Iceberg implementation or official build target. I did not attempt to install an extension from the browser or compile a custom WASM extension. Therefore:

- **Native Iceberg scan via latest metadata JSON:** supported by the project's existing documented workflow; exact metadata HEAD is public.
- **Iceberg from DuckDB-Wasm:** **not verified; assume unsupported for demo planning** until an in-browser `INSTALL/LOAD iceberg` and `iceberg_scan(HTTPS metadata href)` test demonstrates otherwise.
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

Do **not** promise browser Iceberg. Prototype a single Vue component querying the exact January parquet object with DuckDB-Wasm. First make a minimal browser probe served from the actual Slidev origin: initialize WASM, query `SELECT count(*) FROM read_parquet('<exact HTTPS object>')`, and record actual network requests and response CORS headers. Then try loading spatial and the bbox query. If any step fails on CORS or WASM extension availability, stop the direct-browser path and choose a controlled remote native-DuckDB endpoint or a static result for the talk. Separately, test Iceberg WASM capability with a tiny standalone query before revisiting the architecture.

Reliability costs: WASM bundle/worker startup, first-load download and browser memory, public bucket CORS policy, attendee Wi-Fi, extension downloads, and presentation-origin differences. Security costs: arbitrary SQL can be computationally abusive if proxied; never put AWS credentials in the browser; server-side option requires strict query limits and read-only access. This spike made no runtime/dependency changes and performed no browser execution.

## Sources and caveats

- [DuckDB-Wasm overview](https://duckdb.org/docs/stable/clients/wasm/overview.html), [WASM extensions](https://duckdb.org/docs/stable/clients/wasm/extensions.html), [Iceberg extension](https://duckdb.org/docs/stable/core_extensions/iceberg/overview.html).
- [Project README](../../../README.md), especially public exact-path layout and existing Iceberg SQL; [`slides.md`](slides.md).
- Development Seed repository/source links above. GitHub tool searches included code results not all authored by Development Seed; only the three repositories explicitly under the `developmentseed` org are cited as examples.
- HTTP checks and native DuckDB execution were performed during this spike. No DuckDB-Wasm browser query was performed. CORS preflight used `https://example.org`, not the deployed Slidev origin; the 403 is a warning, not proof that the deck's origin is denied. An originless range GET is not equivalent to a successful browser preflight.
