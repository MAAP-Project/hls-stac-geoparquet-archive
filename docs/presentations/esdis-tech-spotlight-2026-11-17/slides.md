---
title: HLS STAC GeoParquet Archive
info: An editable introduction for NASA scientists and engineers
theme: ./theme
layout: cover
class: text-center
transition: slide-left
mdc: true
---

# HLS STAC GeoParquet Archive

## A queryable metadata snapshot for HLS granules

MAAP Project · Introduction for NASA scientists and engineers

---
layout: default
---

# The archive, in one sentence

A periodically refreshed, unofficial copy of **Harmonized Landsat Sentinel-2 (HLS) granule STAC metadata**, stored as GeoParquet and published with static Iceberg metadata for analytical queries.

- Collections: `HLSL30` and `HLSS30` (HLS 2.0)
- The archive contains **metadata and asset references**, not copies of the HLS raster imagery.
- CMR remains the canonical, dynamic catalog; this archive is a convenient snapshot, not a replacement.

---
layout: default
---

# Why columnar STAC metadata?

STAC describes geospatial assets and their properties. GeoParquet makes those records practical to scan and analyze with columnar tools.

- Read only the fields needed for a question rather than fetching many individual STAC documents.
- Keep geometry and spatial metadata alongside the tabular item properties.
- Organize files by collection, year, and month for bounded, partition-aware queries.
- Use familiar analytics clients such as DuckDB and PyArrow.

This is a data-access and metadata organization choice. It does not change HLS pixel values or provide a raster processing service.

---
layout: default
---

# From CMR to an analytical snapshot

<div class="pipeline">
  <div class="pipeline-step"><strong>1 · CMR</strong>Query HLS granules by collection and date.</div>
  <div class="pipeline-arrow">→</div>
  <div class="pipeline-step"><strong>2 · Daily links</strong>Cache STAC JSON links by day.</div>
  <div class="pipeline-arrow">→</div>
  <div class="pipeline-step"><strong>3 · Monthly GeoParquet</strong>Write partitioned columnar metadata.</div>
  <div class="pipeline-arrow">→</div>
  <div class="pipeline-step"><strong>4 · Static Iceberg</strong>Publish table metadata for exact file locations.</div>
</div>

<div class="mt-8 text-sm text-muted">
  DuckDB can read the Iceberg metadata and referenced objects without relying on recursive S3 bucket listing.
</div>

---
layout: default
---

# How the snapshot stays current

The deployed workflow runs every five days for both collections:

- Catch up the **previous month** for late-arriving records.
- Refresh the **current month** incrementally.
- Process a month as daily CMR queries → one monthly GeoParquet write → Iceberg metadata publication.
- Run historical backfills month by month to avoid concurrent updates to a collection's Iceberg metadata.

The result is intentionally a snapshot. Recent months can be incomplete, and records may differ from a live CMR query.

---
layout: default
---

# Anonymous DuckDB setup

The archive objects are publicly readable, so these queries do not need a credential chain or bucket listing. Use a fresh DuckDB connection and an anonymous S3 secret:

```sql
INSTALL httpfs;
LOAD httpfs;
INSTALL iceberg;
LOAD iceberg;

CREATE SECRET anon_s3 (
  TYPE S3,
  PROVIDER CONFIG,
  KEY_ID '',
  SECRET '',
  REGION 'us-west-2'
);

SET VARIABLE hls_prefix =
  's3://nasa-maap-data-store/file-staging/nasa-map/' ||
  'hls-stac-geoparquet-archive/v2/HLSL30_2.0/';
```

The direct query reads an exact monthly object key. For the six-month example, it lists six month-specific Parquet keys under `hls_prefix`; the Iceberg query keeps using only `latest.metadata.json`.

---
layout: default
class: sql-walkthrough
---

# Direct scan: exact January Parquet key

```sql {1-12|2-8|9-10|11-12|1|1-12}
SELECT count(*) AS items
FROM read_parquet(
  's3://nasa-maap-data-store/' ||
  'file-staging/nasa-map/' ||
  'hls-stac-geoparquet-archive/v2/' ||
  'HLSL30_2.0/year=2024/month=1/' ||
  'HLSL30_2.0-2024-1.parquet'
)
WHERE datetime >= TIMESTAMPTZ '2024-01-01 00:00:00+00'
  AND datetime < TIMESTAMPTZ '2024-02-01 00:00:00+00'
  AND bbox.xmin <= -90 AND bbox.xmax >= -100
  AND bbox.ymin <= 50 AND bbox.ymax >= 40;
```

<div class="query-capture">
  <div>
    <strong>Captured DuckDB output</strong>
    <pre class="duckdb-output">┌───────┐
│ items │
│ int64 │
├───────┤
│   550 │
└───────┘</pre>
  </div>
  <p><strong>Observed query wall time: 7.761 s</strong><br><small>DuckDB 1.5.5 · one run · 2026-11-16</small></p>
</div>

<v-switch at="1">
  <template #1><p><strong>Source:</strong> This exact January object key needs no bucket listing.</p></template>
  <template #2><p><strong>Time:</strong> Include Jan 1 UTC; exclude Feb 1 UTC.</p></template>
  <template #3><p><strong>Spatial:</strong> Keep bbox envelopes overlapping (-100, 40, -90, 50), not exact geometry intersections.</p></template>
  <template #4><p><strong>Count:</strong> Count records that satisfy both filters.</p></template>
  <template #5><p>The complete query combines the exact key, half-open month, envelope overlap, and count.</p></template>
</v-switch>

<div class="query-caveat">Timer covers execute + fetchone only; setup and output formatting excluded. Single run, with no cache or network conditions guaranteed.</div>

---
layout: default
class: sql-walkthrough
---

# Iceberg scan: static metadata href

```sql {1-12|2-8|9-10|11-12|1|1-12}
SELECT count(*) AS items
FROM iceberg_scan(
  's3://nasa-maap-data-store/' ||
  'file-staging/nasa-map/' ||
  'hls-stac-geoparquet-archive/v2/' ||
  'HLSL30_2.0/iceberg/metadata/' ||
  'latest.metadata.json'
)
WHERE datetime >= TIMESTAMPTZ '2024-01-01 00:00:00+00'
  AND datetime < TIMESTAMPTZ '2024-02-01 00:00:00+00'
  AND bbox.xmin <= -90 AND bbox.xmax >= -100
  AND bbox.ymin <= 50 AND bbox.ymax >= 40;
```

<div class="query-capture">
  <div>
    <strong>Captured DuckDB output</strong>
    <pre class="duckdb-output">┌───────┐
│ items │
│ int64 │
├───────┤
│   550 │
└───────┘</pre>
  </div>
  <p><strong>Observed query wall time: 9.846 s</strong><br><small>DuckDB 1.5.5 · one run · 2026-11-16</small></p>
</div>

<v-switch at="1">
  <template #1><p><strong>Source:</strong> Start at the metadata href; Iceberg follows its manifest references to data files.</p></template>
  <template #2><p><strong>Time:</strong> Include Jan 1 UTC; exclude Feb 1 UTC.</p></template>
  <template #3><p><strong>Spatial:</strong> Keep bbox envelopes overlapping (-100, 40, -90, 50), not exact geometry intersections.</p></template>
  <template #4><p><strong>Count:</strong> Count records that satisfy both filters.</p></template>
  <template #5><p>The complete query combines the metadata href, half-open month, envelope overlap, and count.</p></template>
</v-switch>

<div class="query-caveat">Timer covers execute + fetchone only; setup and output formatting excluded. Single run, with no cache or network conditions guaranteed.</div>

---
layout: default
class: sql-walkthrough
---

# Direct scan: six explicit monthly objects

```sql {1-13|2-9|10-11|12-13|1|1-13}
SELECT count(*) AS items
FROM read_parquet([
  getvariable('hls_prefix') || 'year=2024/month=1/HLSL30_2.0-2024-1.parquet',
  getvariable('hls_prefix') || 'year=2024/month=2/HLSL30_2.0-2024-2.parquet',
  getvariable('hls_prefix') || 'year=2024/month=3/HLSL30_2.0-2024-3.parquet',
  getvariable('hls_prefix') || 'year=2024/month=4/HLSL30_2.0-2024-4.parquet',
  getvariable('hls_prefix') || 'year=2024/month=5/HLSL30_2.0-2024-5.parquet',
  getvariable('hls_prefix') || 'year=2024/month=6/HLSL30_2.0-2024-6.parquet'
])
WHERE datetime >= TIMESTAMPTZ '2024-01-01 00:00:00+00'
  AND datetime < TIMESTAMPTZ '2024-07-01 00:00:00+00'
  AND bbox.xmin <= -90 AND bbox.xmax >= -100
  AND bbox.ymin <= 50 AND bbox.ymax >= 40;
```

<div class="query-capture">
  <div>
    <strong>Captured DuckDB output</strong>
    <pre class="duckdb-output">┌───────┐
│ items │
│ int64 │
├───────┤
│  5101 │
└───────┘</pre>
  </div>
  <p><strong>Observed query wall time: 10.369 s</strong><br><small>DuckDB 1.5.5 · one run · 2026-11-16</small></p>
</div>

<v-switch at="1">
  <template #1><p><strong>Source:</strong> The six exact keys are enumerated; direct access does not discover months with a bucket listing.</p></template>
  <template #2><p><strong>Time:</strong> Include Jan 1 UTC; exclude Jul 1 UTC.</p></template>
  <template #3><p><strong>Spatial:</strong> Keep bbox envelopes overlapping (-100, 40, -90, 50), not exact geometry intersections.</p></template>
  <template #4><p><strong>Count:</strong> Count records that satisfy both filters across the six files.</p></template>
  <template #5><p>Six known monthly files spell out the direct source list; no wildcard expansion or listing.</p></template>
</v-switch>

<div class="query-caveat">Timer covers execute + fetchone only; setup and output formatting excluded. Single run, with no cache or network conditions guaranteed.</div>

---
layout: default
class: sql-walkthrough
---

# Iceberg scan: same six-month window

```sql {1-8|2-4|5-6|7-8|1|1-8}
SELECT count(*) AS items
FROM iceberg_scan(
  getvariable('hls_prefix') || 'iceberg/metadata/latest.metadata.json'
)
WHERE datetime >= TIMESTAMPTZ '2024-01-01 00:00:00+00'
  AND datetime < TIMESTAMPTZ '2024-07-01 00:00:00+00'
  AND bbox.xmin <= -90 AND bbox.xmax >= -100
  AND bbox.ymin <= 50 AND bbox.ymax >= 40;
```

<div class="query-capture">
  <div>
    <strong>Captured DuckDB output</strong>
    <pre class="duckdb-output">┌───────┐
│ items │
│ int64 │
├───────┤
│  5101 │
└───────┘</pre>
  </div>
  <p><strong>Observed query wall time: 12.105 s</strong><br><small>DuckDB 1.5.5 · one run · 2026-11-16</small></p>
</div>

<v-switch at="1">
  <template #1><p><strong>Source:</strong> The same metadata href identifies the Iceberg table; its manifests direct the scan to files.</p></template>
  <template #2><p><strong>Time:</strong> Include Jan 1 UTC; exclude Jul 1 UTC.</p></template>
  <template #3><p><strong>Spatial:</strong> Keep bbox envelopes overlapping (-100, 40, -90, 50), not exact geometry intersections.</p></template>
  <template #4><p><strong>Count:</strong> Count records that satisfy both filters across the six months.</p></template>
  <template #5><p>Iceberg keeps one metadata entrypoint while its published references identify the monthly data files.</p></template>
</v-switch>

<div class="query-caveat">Timer covers execute + fetchone only; setup and output formatting excluded. Single run, with no cache or network conditions guaranteed.</div>

---
layout: default
---

# What exists today

- CLI commands to cache daily CMR STAC links, write monthly GeoParquet, and publish or repair static Iceberg metadata.
- AWS Lambda + Step Functions orchestration for scheduled monthly work and sequential historical backfills.
- Local processing support and a MinIO pipeline check for exercising the storage/write/read path.
- Documentation notebook examples for inspecting the archive and comparing it with CMR.

## Current constraints

This is an unofficial, periodically refreshed metadata copy. It is not guaranteed complete, especially for recent months. Use live CMR when current catalog completeness matters.

---
layout: default
---

# Discussion

The archive separates three concerns:

- **Discovery:** query CMR or analyze an archived catalog snapshot.
- **Metadata storage:** read STAC item records as partitioned GeoParquet.
- **Science data access:** retrieve and process HLS assets through their supported access paths.

Useful next questions: which archive fields and time ranges matter to your workflow, and where does a snapshot fit alongside live CMR queries?

---
layout: default
---

# Project links

- [Repository](https://github.com/MAAP-Project/hls-stac-geoparquet-archive)
- [Project documentation](https://maap-project.github.io/hls-stac-geoparquet-archive/)
- [Notebook: archive access and CMR comparison](https://maap-project.github.io/hls-stac-geoparquet-archive/)

<small>Starter deck; update the content and presentation details for your audience.</small>
