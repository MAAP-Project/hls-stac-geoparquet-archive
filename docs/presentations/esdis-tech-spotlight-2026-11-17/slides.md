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
```

The direct query reads the exact monthly object. The Iceberg query follows only the published metadata file to locate data objects.

---
layout: two-cols-header
layoutClass: query-results
---

# Direct scan: exact monthly Parquet key

::left::

```sql {1-13|2-8|9-10|11-12|1,13|1-13}
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

::right::

<div class="query-result">
  <strong>Result: 550 items</strong>
  <span>Observed runtime: 7.260 s</span>
  <small>One run · DuckDB 1.5.5 · 2026-11-16</small>
</div>

<v-switch at="1">
  <template #1>
    <p><strong>Source:</strong> The exact January Parquet key names one object directly; no bucket listing or wildcard expansion.</p>
  </template>
  <template #2>
    <p><strong>Time:</strong> Include Jan 1 at 00:00 UTC and exclude Feb 1 at 00:00 UTC.</p>
  </template>
  <template #3>
    <p><strong>Spatial filter:</strong> Keep bbox envelopes overlapping (-100, 40, -90, 50), not exact geometry intersections.</p>
  </template>
  <template #4>
    <p><strong>Count:</strong> Return the number of records satisfying both filters.</p>
  </template>
  <template #5>
    <p>The full query combines the exact source, half-open month, bbox-envelope overlap, and count.</p>
  </template>
</v-switch>

<div class="query-caveat">Single-run wall time around execute + fetchone only; setup excluded. No warm/cold cache guarantee or benchmark claim.</div>

---
layout: two-cols-header
layoutClass: query-results
---

# Iceberg scan: static metadata href

::left::

```sql {1-13|2-8|9-10|11-12|1,13|1-13}
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

::right::

<div class="query-result">
  <strong>Result: 550 items</strong>
  <span>Observed runtime: 9.756 s</span>
  <small>One run · DuckDB 1.5.5 · 2026-11-16</small>
</div>

<v-switch at="1">
  <template #1>
    <p><strong>Source:</strong> Start at only the published metadata href; Iceberg follows its manifest references to data files.</p>
  </template>
  <template #2>
    <p><strong>Time:</strong> Include Jan 1 at 00:00 UTC and exclude Feb 1 at 00:00 UTC.</p>
  </template>
  <template #3>
    <p><strong>Spatial filter:</strong> Keep bbox envelopes overlapping (-100, 40, -90, 50), not exact geometry intersections.</p>
  </template>
  <template #4>
    <p><strong>Count:</strong> Return the number of records satisfying both filters.</p>
  </template>
  <template #5>
    <p>The full query combines the metadata href, half-open month, bbox-envelope overlap, and count.</p>
  </template>
</v-switch>

<div class="query-caveat">Single-run wall time around execute + fetchone only; setup excluded. No warm/cold cache guarantee or benchmark claim.</div>

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
