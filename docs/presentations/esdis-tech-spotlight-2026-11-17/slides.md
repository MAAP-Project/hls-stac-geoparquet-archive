---
title: HLS STAC GeoParquet Archive
info: ESDIS Tech Spotlight · November 17, 2026 · Henry Rodman, Development Seed
theme: ./theme
layout: title
transition: slide-left
mdc: true
---

<script setup>
import openingLogo from './public/images/maap-logo.png'
import lenaDelta from './public/images/lena-delta.jpg'
</script>

# HLS STAC<br>GeoParquet<br>Archive

::subtitle::
Lightening the Load on CMR

::logo::
<img :src="openingLogo" alt="NASA MAAP" />

::image::
<img :src="lenaDelta" alt="Satellite view of the Lena River Delta in Siberia" />
<a href="https://github.com/developmentseed/ds-slidev-template/blob/ba87b3b882c19df868e3b834c13be22f7bb05351/public/images/theme/lena-delta.jpg">Lena River Delta</a>

::event::
<p class="event-name">ESDIS Tech Spotlight</p>
<p class="event-date">2026-11-17</p>
<p class="event-presenter">Henry Rodman - Development Seed</p>
<p class="event-affiliation">Office of Data Science and Informatics<br> NASA Marshall Space Flight Center</p>

<!--
Opening · about 30 seconds

Target roughly 37–40 minutes, then 10–15 minutes of discussion.
The revised sequence needs a timed rehearsal; old per-slide timestamps have
been removed. Leave buffer in the one-hour slot rather than filling it.

If running late, summarize daily/monthly ingestion from the overview and
shorten the direct-Parquet walkthrough. Keep the full Iceberg query and the
array payoff. Discussion and backup are outside the presentation clock.
The subtitle states the design goal, not a measured reduction in CMR traffic.

Introduce presenter and project. The story starts with a scientist's need for
long time series, not a choice of storage technology.
-->

---
layout: default
class: photo-section
title: Why do we need a separate metadata archive when we have CMR?
---

<script setup>
import backgroundImage from './public/images/landsat8-ord-river-australia.jpg'
</script>

<img :src="backgroundImage" alt="Landsat 8 view of the Ord River, Australia" class="section-photo" />
<div class="section-copy"><h1>Why do we need a separate metadata archive when we have CMR?</h1></div>
<div class="section-credit"><a href="https://unsplash.com/@usgs">USGS</a> on <a href="https://unsplash.com/photos/2BThgnOYoIc">Unsplash</a></div>

<!--
Image transition · about 15 seconds
Introduce the first section in one sentence. The image is a section divider,
not the study area or output of the illustrative biomass campaign.
-->

---
layout: default
class: code-slide
title: What is the archive?
---

# What is the archive?

**Two Hive-partitioned STAC GeoParquet datasets in S3.** One row per granule.

```text
s3://nasa-maap-data-store/
└── file-staging/nasa-map/hls-stac-geoparquet-archive/v2/
    ├── HLSL30_2.0/
    │   ├── year=2024/
    │   │   ├── month=1/HLSL30_2.0-2024-1.parquet
    │   │   ├── month=2/HLSL30_2.0-2024-2.parquet
    │   │   └── …
    │   └── iceberg/metadata/latest.metadata.json
    └── HLSS30_2.0/
        ├── year=2024/
        │   ├── month=1/HLSS30_2.0-2024-1.parquet
        │   └── …
        └── iceberg/metadata/latest.metadata.json
```

<div class="takeaway">Metadata and raster links, not imagery. Read Parquet directly or discover its files through Iceberg.</div>

<!--
Concrete archive overview · about 1 minute
- Establish the physical deliverable before showing a client: ordinary objects
  in the MAAP S3 bucket, us-west-2. This tree shows example paths, not an inventory.
- Each collection has one file per month, partitioned by year and unpadded month.
  Ellipses omit other months/years and additional Iceberg metadata files.
- The static Iceberg entrypoint references the same Parquet data, not a second
  copy of the records. Its manifests are omitted from this simplified tree.
- Producer-derived STAC properties, footprints, and nested asset links live here;
  HLS raster assets remain upstream. This is an unofficial metadata copy.
- Exact objects are public; anonymous bucket listing is not available.
- Source: docs/index.ipynb overview. Freshness and publication details come later.
Transition: here is a query against one of those exact files.
-->

---
layout: default
class: rustac-slide
title: A sneak peek — query the archive with rustac
---

# Query the metadata archive without an API

```python
from rustac import DuckdbClient

href = (
    "https://nasa-maap-data-store.s3.us-west-2.amazonaws.com/"
    "file-staging/nasa-map/hls-stac-geoparquet-archive/v2/"
    "HLSL30_2.0/year=2024/month=1/HLSL30_2.0-2024-1.parquet"
)
items = DuckdbClient().search(
    href,
    bbox=(-100, 40, -90, 50),
    datetime="2024-01-01T00:00:00Z/2024-01-31T23:59:59Z",
    max_items=3,
)
```


<!--
Show the complete URL rather than hiding archive access behind a variable.
No Earthdata credentials for this metadata query; imagery access is separate.
These are reconstructed STAC item dictionaries, not byte-for-byte producer JSON.
The three-item cap is not the total count or a benchmark.
Transition: who needs this, and what are they doing with HLS?
-->

---
layout: default
class: maap-context-slide
title: MAAP
---

<script setup>
import maapLogo from './public/images/maap-logo.png'
</script>

<img :src="maapLogo" alt="NASA MAAP logo" class="context-logo" />
<div class="footnote">MAAP: Multi-Mission Algorithm and Analysis Platform</div>

<!--
Introduce the platform before the imagery. This is the NASA-branded site logo,
not a joint NASA/ESA lockup. MAAP supplies the science and compute context.
-->

---
layout: default
class: hls-context-slide
title: HLS
---

<script setup>
import hlsConstellations from './public/images/hls-constellations.png'
</script>

<div class="hls-context-copy">

# HLS

## Harmonized Landsat Sentinel-2

30 m reflectance time series with pixel-level quality masks.

Landsat-derived **L30**<br>Sentinel-2-derived **S30**

This archive: **version 2.0**.

</div>
<div class="hls-context-image">
<img :src="hlsConstellations" alt="The broader HLS Landsat and Sentinel-2 constellation" class="context-constellations" />
<div class="footnote">Illustration: <a href="https://hls.gsfc.nasa.gov/">NASA HLS</a>. Contributions vary by collection and date.</div>
</div>

<!--
HLS context · about 1 minute
- Harmonized Landsat Sentinel-2: reflectance observations from Landsat and
  Sentinel-2, harmonized for richer time series.
- 30 m resolution. Quality masks support cloud/shadow and other filtering.
- HLSL30 is Landsat-derived; HLSS30 is Sentinel-2-derived.
- This archive targets those version 2.0 collections.
- Strong demand is the motivation; do not make an unsourced NASA usage ranking.

Image from the NASA HLS site, published March 2026. It illustrates the broader
HLS constellation, not a claim that every pictured satellite contributes to
every version/date represented by this archive.
Transition: before accessing the pixels, users need to find observations.
-->

---
layout: default
title: Example use case — global biomass modeling
class: biomass-slide
---

<script setup>
import biomassImage from './public/images/boreal-forest-biomass-density.png'
</script>

<div class="eyebrow">The science workflow</div>

# Example use case: global biomass modeling

<p class="biomass-input">Cloud-masked HLS time series for training and prediction.</p>
<div class="biomass-workflow">
<span><strong>Train</strong> at reference locations</span>
<div class="diagram-link"><span>Fitted model</span><i></i></div>
<span><strong>Predict</strong> across tiles</span>
</div>
<figure class="biomass-figure">
<img :src="biomassImage" alt="Circumpolar map of boreal forest aboveground biomass density, with a scale from zero to at least 150 megagrams per hectare" />
<figcaption><a href="https://www.esa.int/ESA_Multimedia/Images/2021/10/Boreal_Forest_Above_Ground_Biomass_Density">Boreal forest biomass · ESA</a> · Illustration, not an output of this HLS workflow.</figcaption>
</figure>

<!--
Use case · about 2 minutes
- Frame a type of MAAP project, not a specific validated campaign or model.
- The ESA boreal biomass map illustrates the science goal; it is not a result
  from the HLS workflow described here.
- Training extracts time series at reference locations. Prediction constructs
  equivalent inputs across a much larger set of locations or spatial tiles.
- At campaign scale, distribute work to meet memory and turnaround constraints.
  A small study does not automatically need a cluster.
- Keep preprocessing consistent between training and prediction. Scene-level
  cloud cover cannot replace pixel-level quality masks.
- This is the context, not a biomass tutorial. Move to the shared metadata need.
-->

---
layout: default
title: Why do we need metadata?
---

<div class="eyebrow">Before reading pixels</div>

# Why do we need metadata?

<div class="metadata-journey">
<div class="journey-band">STAC metadata identifies observations and describes how to load them</div>
<div class="journey-pixels">Pixel computation</div>
<section class="diagram-process"><span class="diagram-kicker">01 · Discover</span><strong>Find observations</strong><p>Times, footprints, bands,<br>and raster asset links</p></section>
<div class="diagram-link"><i></i></div>
<section class="diagram-process"><span class="diagram-kicker">02 · Construct</span><strong>Build a lazy array</strong><p>Place observations in space<br>and time with a loader</p></section>
<div class="diagram-link"><i></i></div>
<section class="diagram-process pixel-stage"><span class="diagram-kicker">03 · Compute</span><strong>Read selected pixels</strong><p>Select space and time;<br>run array operations</p></section>
</div>

<div class="takeaway">With STAC metadata users don't need to even think about file paths.</div>

<!--
Lazy means pixel values need not be read just to construct and select an array.
Metadata lookup, coordinate construction, and sometimes COG header reads still
happen. Projection and band metadata help loaders avoid guessing.
Transition: show the workflow scientists want, not another path-management loop.
-->

---
layout: default
class: datacube-slide
title: The workflow users want — a virtual HLS datacube
---

<script setup>
import teaserHtml from './public/examples/hls-array.html?raw'
import teaserStyles from './public/examples/xarray.css?raw'
const teaserDocument = teaserHtml.replace('</body>', `<style>${teaserStyles}</style></body>`)
</script>


# Users want a virtual HLS datacube

<div class="datacube-teaser">
<div>
<div class="panel-label">Desired API · pseudocode</div>

```python
cube = open_hls_cube(
    area=region, dates=date_range,
)
subset = cube.sel(
    time=slice(start, end),
    x=x_window, y=y_window,
)
result = prepare_model_inputs(subset).compute()
```

</div>
<div class="teaser-output">
<div class="array-frame"><iframe :srcdoc="teaserDocument" title="Teaser: saved lazycogs Xarray view of HLS with 365 daily steps and three bands" sandbox="allow-same-origin" /></div>
<p class="footnote">≈15 TB logical view, nothing downloaded... yet!</p>
</div>
</div>

<div class="takeaway">Select an array first. Read pixels when you compute.</div>

<!--
The opening function and prepare_model_inputs are deliberately pseudocode.
The HTML at right is the real saved lazycogs output shown again near the end,
not output from this invented API. It was built from a prepared local index.
Its 365 daily coordinates are a view choice, not guaranteed daily coverage.
No pixel windows were read at opening; metadata queries, coordinate construction,
and COG header reads still happen. Defer implementation/access details until
that later slide. Logical size is not transferred bytes.
For odc-stac, query items first and pass them to odc.stac.load with Dask chunks;
it does not imply deferred metadata discovery. lazycogs can defer searches to
selected spatial/time chunks. Neither route removes raster authentication.
Use the same masking/scaling in training and prediction.
Transition: the user-facing workflow is straightforward. Getting suitable HLS
metadata efficiently is the part we need to improve.
-->

---
layout: default
class: photo-section
title: HLS metadata today
---

<script setup>
import metadataImage from './public/images/landsat8-ord-river-australia.jpg'
</script>

<img :src="metadataImage" alt="Landsat 8 view of the Ord River, Australia" class="section-photo" />
<div class="section-copy"><h1>HLS metadata today</h1></div>
<div class="section-credit"><a href="https://unsplash.com/@usgs">USGS</a> on <a href="https://unsplash.com/photos/2BThgnOYoIc">Unsplash</a></div>

<!--
Introduce the content and retrieval problems separately. This image is
illustrative, not a campaign study area or archive coverage map.
-->

---
layout: default
class: comparison-slide
title: The same granule, different STAC records
clicks: 3
---

# The same granule, different STAC records

<div class="record-id">HLS.L30.T14TQT.2024001T170441.v2.0</div>

<div class="metadata-inspector">
<div class="metadata-options">
<table>
<thead><tr><th>Representation</th><th>Red asset</th></tr></thead>
<tbody>
<tr><td><span v-mark.underline.orange="{ at: [1, 2] }">CMR API granule search, STAC output format</span></td><td><code>data9</code></td></tr>
<tr><td><span v-mark.underline.orange="{ at: [2, 3] }">CMR STAC</span></td><td><code>B04</code></td></tr>
<tr><td><span v-mark.underline.orange="{ at: 3 }">HLS producer item — preferred</span></td><td><code>B04</code></td></tr>
</tbody>
</table>
<p class="producer-link">Each granule's related links include the HLS-produced STAC item.</p>
</div>
<div class="metadata-preview">

````md magic-move {at:2, lines: true}
```json
{
  "assets": {
    "data9": {
      "href": "…v2.0.B04.tif",
      "title": "Download …v2.0.B04.tif"
    }
  }
}
```
```json
{
  "assets": {
    "B04": {
      "href": "…v2.0.B04.tif",
      "roles": ["data"]
    }
  }
}
```
```json
{
  "properties": {
    "proj:epsg": 32614,
    "proj:shape": [3660, 3660]
  },
  "assets": {
    "B04": {
      "eo:bands": [{"name": "B04", "common_name": "red"}],
      "href": "…v2.0.B04.tif"
    }
  }
}
```
````

<p v-if="$clicks < 2" class="metadata-verdict bad">Generic asset keys; no producer projection fields or band descriptions.</p>
<p v-else-if="$clicks < 3" class="metadata-verdict bad">Better asset keys; still missing producer projection fields and band descriptions.</p>
<p v-else class="metadata-verdict good">Projection + band metadata: the richest item we have. This is what we archive.</p>
</div>
</div>
<div class="takeaway">Retrieving STAC metadata from CMR is problematic: its STAC outputs omit useful producer fields.</div>

<!--
Record comparison · about 2.5 minutes
- Normal CMR granule search returns granule records, not STAC items.
- CMR's .stac response and the CMR STAC service are separate routes.
- We captured all three representations for the exact same granule. This table
  reports that case, not every item in every collection.
- The .stac red asset key was data9 in this capture; generic numbering need not
  be stable across results. It points to the same B04 raster.
- CMR STAC exposes B04 plus an s3_B04 asset, but no producer projection fields
  or eo:bands on B04. Clients may need configuration or raster header reads.
- The producer item has proj:epsg=32614, proj:shape=[3660,3660], a transform,
  and the B04 band description. We choose that source for the archive.
- Large searches also involve pagination and service constraints. We have not
  verified a current HLS-specific rate limit or forum citation, so no numerical
  limit or attributed failure claim appears on this slide.

Source endpoints and the example granule are documented in the adjacent README.
Exact request URLs and reproduction notes are in the deck README.
Transition: two separate needs. We want the producer's richer item records,
and we want clients to reuse them without repeating bulk extraction. Choosing
producer STAC addresses content; shared columnar publication addresses retrieval.
-->

---
layout: default
class: photo-section
title: Cloud-optimized data needs cloud-optimized metadata
---

<script setup>
import cloudImage from './public/images/landsat9-apostle-islands-lake-superior.jpg'
</script>

<img :src="cloudImage" alt="Landsat 9 view of the Apostle Islands, Lake Superior" class="section-photo" />
<div class="section-copy"><h1>Cloud-optimized data needs cloud-optimized metadata</h1></div>
<div class="section-credit"><a href="https://unsplash.com/@usgs">USGS</a> on <a href="https://unsplash.com/photos/j7HqdQqn7Jo">Unsplash</a></div>

<!--
Transition image only. HLS COGs let clients select pixels in object storage;
we want a similarly reusable way to select observation metadata.
Concept adapted from Pete Gadomski's Cloud-Native Geospatial Metadata talk:
https://www.gadom.ski/presentations/2025-11-04-stac-geoparquet.html#/4/1/0
-->

---
layout: default
class: reuse-slide
title: One bulk extraction, many object-storage readers
clicks: 1
---


# One bulk extraction, many object-storage readers

<div class="reuse-comparison">
<section class="reuse-before">
<h2>Before · repeat discovery</h2>
<div class="reuse-refresh"><span class="diagram-kicker">For every analysis</span><p>Each client searches and paginates.</p></div>
<div class="reuse-source cmr-source"><strong>CMR API</strong><span>Live granule discovery</span></div>
<div class="reader-links"><span>Search</span><span>Search</span><span>Search</span></div>
<div class="reader-nodes"><span>Notebook</span><span>Worker</span><span>Campaign</span></div>
<p class="diagram-caption">Overlapping requests repeat upstream work.</p>
</section>
<section v-click="1" class="reuse-after">
<h2>After · reuse discovery</h2>
<div class="reuse-refresh"><span class="diagram-kicker">Once per refresh cycle</span>
<div class="reuse-extraction"><div class="reuse-upstream cmr-source"><strong>CMR API</strong><span>Granule discovery</span></div><div class="diagram-link cmr-search"><span>Search +<br>paginate</span><i></i></div><div class="reuse-upstream"><strong>Archive builder</strong><span>Fetch producer STAC</span></div></div>
<div class="refresh-connector" aria-hidden="true"><span>Publish</span></div></div>
<div class="reuse-source"><strong>Shared archive · S3</strong><span>Published producer-derived records</span></div>
<div class="reader-links"><span>Read</span><span>Read</span><span>Read</span></div>
<div class="reader-nodes"><span>Notebook</span><span>Worker</span><span>Campaign</span></div>
<p class="diagram-caption">Archive queries do not call CMR.</p>
</section>
</div>

<!--
Retrieval paradigm · about 2 minutes
- Both panels keep readers in the same positions. Upward arrows mean access:
  clients search CMR before, or read the shared archive after. The downward
  builder arrow means publication, in the separate refresh-time lane.
- Long, overlapping time series create repeated granule-level metadata requests.
- Database-backed systems can scale. The point is the cost of repeated search
  and serialization, not an assertion that any particular system must fail.
- The after panel keeps CMR in the refresh-time path: the archive builder
  searches and paginates CMR, then fetches producer STAC and publishes to S3.
  The horizontal search arrow points from the builder toward CMR, matching
  the client-access arrows below; the publication arrow points toward S3.
- Publish records once per ingestion/refresh cycle; let many clients reuse them.
- Client engines read selected files/columns and filter. Object storage itself
  is not a search engine. Some planning/compute burden moves to the client.
- Throughput, request costs, network, and client memory remain constraints.
- Live search APIs still serve live discovery and current-catalog requirements.
Transition: how do stored producer JSON documents become a queryable table?
-->

---
layout: default
class: record-mapping-slide
title: From STAC item JSON to GeoParquet — one item, one row
---

<div class="eyebrow">STAC GeoParquet primer · one real HLS granule</div>

# One STAC item → one GeoParquet row

<div class="record-mapping">
<div>
<h3>HLS-produced item.json · excerpt</h3>

```json {all|3-6|7-10|12-13|all}
{
  "id": "HLS.L30.T14TQT.2024001T170441.v2.0",
  "geometry": {"type": "MultiPolygon",
               "coordinates": […]},
  "bbox": [-95.547105, 46.793671,
           -94.864742, 47.790604],
  "properties": {
    "datetime": "2024-01-01T17:04:41.320673Z",
    "proj:epsg": 32614,
    "eo:cloud_cover": 29.0
  },
  "assets": {
    "B04": {"href": "…v2.0.B04.tif"}
  }
}
```

</div>
<div class="record-row-preview">
<h3>One Parquet row · expanded vertically</h3>
<table>
<thead><tr><th>Column</th><th>Value</th></tr></thead>
<tbody>
<tr><td>id</td><td>HLS.L30.T14TQT.…v2.0</td></tr>
<tr><td>geometry</td><td>WKB binary · MultiPolygon</td></tr>
<tr><td>bbox (struct)</td><td>xmin: −95.547105 · ymin: 46.793671<br>xmax: −94.864742 · ymax: 47.790604</td></tr>
<tr><td>datetime</td><td>2024-01-01 17:04:41.320 UTC</td></tr>
<tr><td>proj:epsg</td><td>32614</td></tr>
<tr><td>eo:cloud_cover</td><td>29.0</td></tr>
<tr><td>assets.B04.href</td><td>…v2.0.B04.tif</td></tr>
</tbody>
</table>
</div>
</div>
<div class="mapping-key"><span>Geometry → WKB</span><span>bbox → struct fields</span><span>Properties → columns</span><span>Assets stay nested</span></div>
<div class="spatial-encoding-note">GeoParquet file metadata describes the geometry encoding and CRS. Filter bounds with <code>bbox.xmin</code>, <code>bbox.ymin</code>, <code>bbox.xmax</code>, <code>bbox.ymax</code>.</div>

<!--
Item-to-table mapping · about 2 minutes
- Follow the same real granule from the earlier metadata comparison. The table
  at right is one row expanded vertically for readability, not a key/value schema.
- STAC defines item content; GeoParquet supplies geospatial columnar storage;
  STAC GeoParquet specifies the mapping. Each additional item supplies another row.
- Click 1: GeoJSON geometry becomes WKB (Well-Known Binary), stored as binary
  values, not the text label shown here. The bbox array [west, south, east, north]
  becomes a struct with numeric xmin/ymin/xmax/ymax fields. Values are from this
  captured granule; the coordinate array is abbreviated, so the JSON is an excerpt.
- GeoParquet's file-level geo metadata describes geometry encoding and CRS.
  These footprint/bbox coordinates are longitude/latitude, not UTM coordinates;
  proj:epsg describes the raster projection, not the footprint's coordinate system.
- Named bbox fields support envelope filters without decoding each WKB geometry.
  Envelope overlap is not exact polygon intersection. Pruning depends on stored
  statistics, layout, and engine support; the bbox alone is not a spatial index.
- Click 2: properties become columns. Click 3: assets stay nested; the dotted
  assets.B04.href label shows a field inside that structure, not a flattened column.
- The archive adds collection=HLSL30_2.0. Raster pixels remain in upstream assets.
- The observed timestamp changes from microseconds to milliseconds. This is
  producer-derived content, not a byte-for-byte copy.
- Discovery and item content have different owners/endpoints. We reuse the
  producer's item rather than reconstructing it from a CMR conversion.
- Full producer and CMR excerpts are in backup; retain stable producer URLs,
  not temporary delivery redirects, when publishing examples.
- Original explanatory layout, not a copied presentation image. Credit Pete's
  linked item-to-table primer for the concepts.
Transition: why is this columnar representation useful to readers?
-->

---
layout: default
class: projection-slide
title: Why the table representation helps
clicks: 1
---

<div class="eyebrow">STAC GeoParquet · selective metadata access</div>

# Why the table representation helps

<div class="projection-layout">
<section class="panel"><div class="panel-label">Select observations and fields</div><p>Filter by time, area, and scene properties.</p><p>No STAC item JSON travels over the network.</p><p>Read Parquet column chunks from selected row groups instead.</p></section>
<section class="projection-demo">

````md magic-move
```sql
SELECT *
FROM hls;
```
```sql
SELECT id, datetime, "eo:cloud_cover"
FROM hls;
```
````

<div class="projection-results" :class="{ 'projected': $clicks >= 1 }" role="table" aria-label="Illustrative columns read; grayed columns are not read after projection">
<div class="projection-result-row" role="row"><div role="columnheader">id</div><div role="columnheader">datetime</div><div role="columnheader">eo:cloud_cover</div><div class="unselected-column" role="columnheader">geometry</div><div class="unselected-column" role="columnheader">assets</div></div>
<div v-for="(cloud, index) in [12, 3, 47]" :key="index" class="projection-result-row" role="row"><div role="cell">scene-{{ ['a', 'b', 'c'][index] }}</div><div role="cell">Jan {{ index + 1 }}</div><div role="cell">{{ cloud }}</div><div class="unselected-column" role="cell">WKB</div><div class="unselected-column" role="cell">{ … }</div></div>
</div>
<p class="projection-caption">{{ $clicks < 1 ? 'All columns returned' : 'Gray = not read · only three columns returned' }}</p>
</section>
</div>
<div class="takeaway">Read the fields you need. Leave the other columns in object storage.</div>

<!--
Start with SELECT * and the full illustrative table. On click, Magic Move changes
SQL to three named columns; geometry and assets remain visible but gray out.
This is a read/skip illustration, not the literal projected result schema: the
skipped columns are not returned, and the gray values do not represent nulls.
The real archive has more columns than this simplified five-column illustration.
Projection lets capable clients skip unneeded column chunks, not just hide them
after downloading everything. Parquet footer/statistics requests still occur.
No STAC item JSON travels over the network when querying the archive.
Separate content (STAC) from representation (GeoParquet) and publication (Iceberg).
Nested assets still contain raster URLs. This is not an imagery mirror.
Concepts adapted from Pete Gadomski's cloud-native metadata talk, linked on
the preceding item-to-table slide.
Transition: how do we prepare and publish these tables?
-->

---
layout: default
class: photo-section
title: Architecture — preparing the shared archive
---

<script setup>
import architectureImage from './public/images/landsat9-bangladesh-coast.jpg'
</script>

<img :src="architectureImage" alt="Landsat 9 view of the Bangladesh coast" class="section-photo" />
<div class="section-copy"><h1>Preparing the shared archive</h1><p>From upstream discovery to reusable records.</p></div>
<div class="section-credit"><a href="https://unsplash.com/@usgs">USGS</a> on <a href="https://unsplash.com/photos/eGGENWtikd0">Unsplash</a></div>

<!--
Architecture transition · about 15 seconds
Now move from the reader's needs to how we prepare the shared metadata.
The image is illustrative, not an archive coverage map.
-->

---
layout: default
class: archive-walkthrough-slide
title: From CMR discovery to reusable tables
clicks: 4
---

<div class="eyebrow">Archive architecture</div>

# From CMR discovery to reusable tables

<div class="archive-walkthrough">
<ol class="archive-steps" aria-label="Archive preparation steps">
<li v-for="(step, index) in ['Query CMR granules', 'Extract STAC links', 'Stash links by day', 'Read a month of links', 'Write monthly Parquet']" :key="step" :class="{ active: $clicks === index, complete: $clicks > index }" :aria-current="$clicks === index ? 'step' : undefined"><span class="archive-step-number">{{ index + 1 }}</span><span>{{ step }}</span></li>
</ol>
<div class="archive-data-view" aria-live="polite">
<Transition name="archive-preview" mode="out-in">
<div v-if="$clicks === 0" key="granules" class="archive-preview">
<div class="diagram-kicker">CMR response · one collection / day</div>
<h2>Granule records</h2>

```json
{
  "title": "HLS.L30.T14TQT.…v2.0",
  "links": [
    { "href": "…B04.tif" },
    { "href": "…v2.0_stac.json" }
  ]
}
```

<p>Search and paginate for the day's granules.</p>
</div>
<div v-else-if="$clicks === 1" key="extract" class="archive-preview">
<div class="diagram-kicker">From each granule's related links</div>
<h2>Keep the producer STAC URL</h2>
<div class="archive-link-choice discarded">…v2.0.B04.tif</div>
<div class="archive-link-choice selected">…v2.0_stac.json</div>
<p>The link points to the HLS-produced item.</p>
</div>
<div v-else-if="$clicks === 2" key="daily" class="archive-preview">
<div class="diagram-kicker">S3 · daily link cache</div>
<h2>One URL list per day</h2>
<div class="archive-path">links/HLSL30_2.0/2024/01/</div>
<div class="archive-day-files"><span class="selected">2024-01-01.json</span><span>2024-01-02.json</span><span>…</span></div>

```json
[
  "https://…/scene-a_stac.json",
  "https://…/scene-b_stac.json"
]
```

<p>Save links, not full STAC items.</p>
</div>
<div v-else-if="$clicks === 3" key="month" class="archive-preview">
<div class="diagram-kicker">Monthly writer · January 2024</div>
<h2>Gather links, fetch items</h2>
<div class="archive-month-inputs"><span>Jan 01</span><span>Jan 02</span><span>…</span><span>Jan 31</span></div>
<div class="archive-fetch-arrow">Daily URL lists ↓</div>
<div class="archive-producer-fetch"><strong>Fetch HLS producer STAC</strong><div class="archive-item-docs"><span>{ scene-a }</span><span>{ scene-b }</span><span>{ … }</span></div></div>
<p>Read the month's cached links; retrieve the item JSON.</p>
</div>
<div v-else key="parquet" class="archive-preview">
<div class="diagram-kicker">S3 · monthly GeoParquet</div>
<h2>Items become rows</h2>
<div class="archive-path">HLSL30_2.0/year=2024/month=1/<br><strong>HLSL30_2.0-2024-1.parquet</strong></div>
<table><thead><tr><th>id</th><th>datetime</th><th>assets</th></tr></thead><tbody><tr><td>scene-b</td><td>Jan 02</td><td>{ … }</td></tr><tr><td>scene-a</td><td>Jan 01</td><td>{ … }</td></tr><tr><td>…</td><td>…</td><td>{ … }</td></tr></tbody></table>
<p>One spatially ordered file per collection / month.</p>
</div>
</Transition>
</div>
</div>

<!--
Architecture walkthrough · about 1.5 minutes · four clicks, five stages
- Start: query CMR for one collection and UTC day; follow pagination. The response
  preview abbreviates and omits fields, and represents one granule, not the full
  CMR response envelope.
- Click 1: extract each granule's producer STAC JSON link, not a raster URL.
- Click 2: cache a JSON array of URLs in S3 for each day. Paths shown use the actual
  padded daily-cache convention. These are links, not cached item documents.
- Click 3: read the daily link files for a month, then fetch the full STAC items
  from HLS producer endpoints. Only discovery queries the CMR API.
- Click 4: flatten properties and write spatially ordered rows to one Parquet
  object per collection/month. Parquet month keys are unpadded.
- This is the preparation path; Iceberg publication is explained separately.
- The primer already showed a real item-to-row conversion. Continue with how
  those rows are assembled into the monthly publication unit.
-->

---
layout: default
class: monthly-concat-slide
title: Concatenate all STAC items per month
---

<div class="eyebrow">Ingestion · one collection, one month</div>

# Concatenate all STAC items per month

<div class="month-assembly">
<div class="daily-items"><span class="diagram-kicker">Many producer items</span><div v-for="day in ['Day 1', 'Day 2', '…', 'Day 31']" :key="day"><span>{{ day }}</span><span class="item-doc">{ }</span><span class="item-doc">{ }</span><span class="item-doc">{ }</span></div><p>Discovered through<br>the daily URL cache</p></div>
<div class="diagram-link"><i></i></div>
<div class="diagram-process"><span class="diagram-kicker">Monthly writer</span><strong>Items → rows</strong><p>Flatten properties<br>Concatenate rows<br>Spatially order</p></div>
<div class="diagram-link"><i></i></div>
<div class="diagram-file monthly-output"><span class="diagram-kicker">One collection / month</span><strong>GeoParquet</strong><table><thead><tr><th>id</th><th>datetime</th><th>assets</th></tr></thead><tbody><tr v-for="row in [4, 1, 6, 2]" :key="row"><td>item-{{ row }}</td><td>Jan …</td><td>{ … }</td></tr><tr><td>…</td><td>…</td><td>…</td></tr></tbody></table><small>Rows ordered by location, not day</small></div>
</div>

```text
HLSL30_2.0/year=2024/month=1/HLSL30_2.0-2024-1.parquet
```


<!--
Daily/monthly ingestion · about 1.5 minutes
- Query CMR by collection and UTC day, following pagination. Cache the producer
  links so monthly writing can proceed without more CMR API calls.
- Fetch items in batches from producer links, not through the CMR API.
- The writer spatially orders records using a Hilbert key derived from tile location.
  Mention as an optimization, not a general performance guarantee.
- Parquet month keys are unpadded (month=1). Daily cache path components differ.
- Complete daily caches do not prove every item fetch succeeds or source completeness.
- Exact objects are publicly readable. Bucket listing is not public; anonymous
  **/*.parquet globs are not a supported discovery shortcut.
-->

---
layout: default
class: iceberg-snapshot-slide
title: Iceberg snapshot
clicks: 1
---

<div class="eyebrow">Publication · collection-wide table</div>

# Iceberg snapshot

A snapshot identifies the complete set of data files belonging to a table.

<div class="iceberg-reference-map">
<div class="iceberg-metadata-label diagram-kicker">Published metadata · follow references</div><div class="iceberg-data-label diagram-kicker">Data files · no rewrite</div>
<div class="diagram-file iceberg-table"><strong>Table metadata</strong><code>latest.metadata.json</code><small>Current snapshot</small></div>
<div class="diagram-link"><i></i></div>
<div class="diagram-file"><strong>Manifest list</strong><small>References manifests</small></div>
<div class="diagram-link"><i></i></div>
<div class="diagram-file"><strong>Manifests</strong><small>File paths + statistics</small></div>
<div class="diagram-link"><i></i></div>
<div class="iceberg-file-set"><div class="diagram-file">January.parquet</div><div class="diagram-file">February.parquet</div><div v-click="1" class="diagram-file new-month">March.parquet <span>New</span></div></div>
</div>
<div class="publication-note"><strong>{{ $clicks < 1 ? 'This publisher rebuilds the complete file set on each publication.' : 'Next publication: rebuild the file set, now including March.' }}</strong><span>No maintained incremental snapshot history.</span></div>

<div class="takeaway"><code>table.add_files(…)</code> registers existing files; readers discover them through metadata.</div>

<!--
Iceberg publication · about 2 minutes
- Follow the reference chain from left to right. Filenames January/February/March
  are explanatory labels; actual keys are on the previous slide. Each collection
  has its own latest.metadata.json. On click, March joins the rebuilt published
  file set; this is not a depiction of retained snapshot history.
- After writing the monthly Parquet object, the current publisher lists existing
  collection files, unifies schemas, creates an in-memory Iceberg table, adds
  existing files, and publishes a new static metadata entrypoint. It rebuilds
  the published file set; it is not maintaining a long-lived REST catalog.
- Publisher needs listing; readers follow metadata/manifest paths with GetObject.
- This does not rewrite the existing monthly files into an Iceberg-native layout.
- Serialize publications per collection. Static publication and mutable monthly
  objects do not establish snapshot isolation for concurrent refreshes.
- Simplifies file discovery; it is not a speed or universal compatibility claim.
-->

---
layout: default
class: availability-slide
title: Refresh and current availability
---

<script setup>
import refreshHero from './public/images/landsat9-kangerdlugssuaq-greenland.jpg'
</script>

# Refreshed every 5 days

HLS **L30 / S30 v2.0** · one Parquet file per collection/month

```text
s3://nasa-maap-data-store/file-staging/nasa-map/
hls-stac-geoparquet-archive/v2/
{collection}/year={year}/month={month}/{collection}-{year}-{month}.parquet
```

Collections: `HLSL30_2.0`, `HLSS30_2.0` · Months: `1`–`12` (unpadded)

Public exact-object reads; a static Iceberg entrypoint per collection.

<figure class="availability-hero">
<img :src="refreshHero" alt="Landsat 9 view of Kangerdlugssuaq Glacier, Greenland" />
<figcaption><a href="https://unsplash.com/@usgs">USGS</a> on <a href="https://unsplash.com/photos/PgL1p8TBGNQ">Unsplash</a> · Kangerdlugssuaq Glacier, Greenland</figcaption>
</figure>

<!--
Availability and freshness · about 1.5 minutes
- The Greenland image is illustrative, not an archive coverage map or output.
- Five-day schedule is not a freshness or completeness SLA.
- Catching up the previous month helps with stragglers, but does not guarantee
  detection of older revisions, retractions, or arbitrarily late arrivals.
- No claim of a complete historical mirror without a dated audit.
- Backfills process months sequentially to avoid overlapping publications.
- For a campaign, decide whether this freshness is sufficient and record the
  metadata inputs used. The mutable archive is not an immutable campaign snapshot.
Transition: use the published metadata from a client's point of view.
-->

---
layout: default
class: photo-section
title: Query patterns — using the archive
---

<script setup>
import queryImage from './public/images/landsat9-apostle-islands-lake-superior.jpg'
</script>

<img :src="queryImage" alt="Landsat 9 view of the Apostle Islands, Lake Superior" class="section-photo" />
<div class="section-copy"><h1>Using the archive</h1><p>SQL, STAC items, and a path back to pixels.</p></div>
<div class="section-credit"><a href="https://unsplash.com/@usgs">USGS</a> on <a href="https://unsplash.com/photos/j7HqdQqn7Jo">Unsplash</a></div>

<!--
Query transition · about 15 seconds
Switch from the publisher's workflow to the user's workflow.
-->

---
layout: default
class: sql-walkthrough
title: Six months of HLS — direct Parquet
transition: fade
---

<div class="eyebrow">DuckDB · direct Parquet</div>

# Six months of HLS: direct Parquet

```sql {all|2-9|10-13|all}
SELECT count(*) AS items
FROM read_parquet([
  format(
    'https://nasa-maap-data-store.s3.us-west-2.amazonaws.com/' ||
    'file-staging/nasa-map/hls-stac-geoparquet-archive/v2/HLSL30_2.0/' ||
    'year=2024/month={}/HLSL30_2.0-2024-{}.parquet',
    month, month
  ) FOR month IN range(1, 7)
])
WHERE datetime >= TIMESTAMPTZ '2024-01-01 00:00:00+00'
  AND datetime < TIMESTAMPTZ '2024-07-01 00:00:00+00'
  AND bbox.xmin <= -90 AND bbox.xmax >= -100
  AND bbox.ymin <= 50 AND bbox.ymax >= 40;
```

<p class="query-explanation">Generate six exact URLs. Extending the dates also means updating the file selection.</p>
<div class="result-strip"><strong>5,101 items</strong><span>January–June 2024 · bbox (-100, 40, -90, 50)</span></div>

<!--
The query generates six exact public URLs using DuckDB's list comprehension
and format(). No getvariable setup, glob, or bucket listing.
Native DuckDB/httpfs setup is in backup.
The earlier six-file capture returned 5,101, 10.369 s execute+fetchone; one run,
not a benchmark. No timing claim for this refactored HTTPS query.
SELECT id, datetime, assets would return records rather than this count.
Advance through the highlights, then fade to a full query with a new source.
-->

---
layout: default
class: sql-walkthrough
title: Six months of HLS — Iceberg
transition: fade
---

<div class="eyebrow">DuckDB · static Iceberg</div>

# Six months of HLS: Iceberg

```sql {2-5|all}
SELECT count(*) AS items
FROM iceberg_scan(
  'https://nasa-maap-data-store.s3.us-west-2.amazonaws.com/' ||
  'file-staging/nasa-map/hls-stac-geoparquet-archive/v2/' ||
  'HLSL30_2.0/iceberg/metadata/latest.metadata.json')
WHERE datetime >= TIMESTAMPTZ '2024-01-01 00:00:00+00'
  AND datetime < TIMESTAMPTZ '2024-07-01 00:00:00+00'
  AND bbox.xmin <= -90 AND bbox.xmax >= -100
  AND bbox.ymin <= 50 AND bbox.ymax >= 40;
```

<div class="takeaway">Change the source, keep the filters. Extend the dates without enumerating monthly files.</div>
<div class="result-strip"><strong>5,101 items</strong><span>Same six-month window · metadata identifies the files</span></div>

<!--
Show the whole query, not just a FROM-clause substitution. The fade and source
highlight connect it to the preceding query. Both use identical predicates.
Original observed time 12.105 s: slower than direct Parquet in that run.
The advantage shown is file discovery and simpler client code, not a speedup.
Readers follow exact metadata/manifest/data references without ListBucket.
Transition: return to the virtual datacube we wanted at the start.
-->

---
layout: default
class: lazycogs-slide
title: A year of HLS as an array
---

<script setup>
import arrayHtml from './public/examples/hls-array.html?raw'
import xarrayStyles from './public/examples/xarray.css?raw'
const arrayDocument = arrayHtml.replace('</body>', `<style>${xarrayStyles}</style></body>`)
</script>

<div class="eyebrow">Back to science · lazycogs</div>

# A year of HLS as an array

<div class="array-columns">
<div>

```python
hls = lazycogs.open(
    "s3://nasa-maap-data-store/file-staging/"
    "nasa-map/hls-stac-geoparquet-archive/"
    "v2/HLSS30_2.0/**/*.parquet",
    bbox=(166020, 0, 834000, 9329010),
    crs="EPSG:32615",
    resolution=30,
    time_period="P1D",
    bands=["B04", "B03", "B02"],
    datetime="2025-01-01/2025-12-31",
    filter="proj:epsg = '32615'",
    duckdb_client=client, store=hls_store,
)
```

</div>
<div class="array-frame"><iframe :srcdoc="arrayDocument" title="Saved Xarray HTML representation: a year of HLS, three bands, 15 TB logical size" sandbox="allow-same-origin" /></div>
</div>

<div class="result-strip"><strong>365 daily steps · 3 bands</strong><span>≈15 TB logical view; no pixel windows read at open.</span></div>
<div class="takeaway maap-requirement">Run this S3-glob example on NASA MAAP: it requires AWS credentials with ListBucket access to the MAAP bucket.</div>

<!--
Array payoff · about 2.5 minutes
- Reused code and actual saved HTML from the local lazycogs slidev-docs worktree.
  The HTML has band=3,time=365,y=310967,x=22266,int16, about 15.16 TB decimal.
- This is a sparse daily view over UTM zone 15N, not 15 TB of downloaded data.
- The path is now a credentialed remote S3 glob, intended for NASA MAAP.
  AWS credentials must allow bucket listing. client and hls_store require setup;
  this is not standalone code or a public anonymous glob.
- The saved output came from the prepared local-index version. Changing the
  metadata source to the remote S3 glob has not been rerun here. Do not imply
  this screenshot proves the remote-glob workflow executed.
- Opening still queries metadata, reads COG headers, and builds coordinates.
  The earlier verified example reports no pixel-window reads at opening.
- lazycogs can defer item searches to the spatial/time chunks selected for reading.
- time_period="P1D" chooses a daily view over irregular observation timestamps;
  it does not mean HLS supplies a dense daily observation at every pixel.
  The next slide makes that virtualization choice explicit.
- Say explicitly: the saved output demonstrates array construction, not a
  computed raster result or biomass estimate, and not Iceberg support.
- A small .sel(...).compute() triggers query, asset selection, and raster work.
  For a study site, select its projected x/y window and dates before computing.
  In the opening campaign, a worker would also read quality-mask assets, mask
  clouds, and apply reflectance scaling to construct training or prediction inputs.
  This RGB example does not load quality masks and is not cloud-masked output.
- The useful result here is a selectable daily view; its logical size is not a
  performance metric. A computed image/time series would be a useful future demo,
  but no such output was verified for this revision.
- Public metadata does not grant public raster access. Earthdata credentials
  apply; direct HLS S3 access requires appropriate us-west-2 compute. Use the
  HTTPS workflow elsewhere: https://developmentseed.org/lazycogs/notebooks/hls-https/
- Reflectance scaling, quality masks, and scientific interpretation remain the
  user's responsibility. Logical array size is not transferred bytes.
-->

---
layout: default
title: One metadata archive, different virtual arrays
---

# One metadata archive, different virtual arrays

HLS is collected continuously. Granules have distinct observation timestamps, not a shared dense time axis.

<div class="next-steps">
  <section><span>Observations</span><h2>Keep the records</h2><p>The archive preserves granule-level times, footprints, and raster links.</p></section>
  <section><span>View policy</span><h2>Choose the time axis</h2><p>Retain acquisition times or group observations into time bins. Decide how to handle gaps and overlaps.</p></section>
  <section><span>lazycogs example</span><h2>Build on demand</h2><p>Query the metadata to construct a daily virtual view. Read selected pixels when computing.</p></section>
</div>

<div class="takeaway">The archive supports array construction without committing everyone to one time-axis policy.</div>

<!--
Virtualization · about 1 minute
- Continuously collected tiled imagery does not naturally form one dense array.
  Granule timestamps differ across the footprint; keeping every timestamp can
  create a sparse global time dimension. Do not assert global timestamp uniqueness.
- We are still working out the right representation for HLS. Daily bins are one
  useful choice, not a settled canonical cube or a guarantee of daily coverage.
- The preceding lazycogs example chooses time_period="P1D" on the fly. Its daily
  coordinate does not by itself specify a scientific compositing operation.
- Binning, overlapping assets, missing observations, masking, and resampling need
  explicit loader/science policies. The archive supplies records, not those policies.
- This is a related virtualization direction, not a claim that lazycogs consumes
  icestac or the archive's Iceberg entrypoints in the demonstrated example.
Transition: flexible views need reusable metadata. How could that metadata stay
current as HLS continues to arrive?
-->

---
layout: default
title: From a monthly archive to a live STAC catalog
---


# Next steps: from a monthly archive to a live STAC catalog

<div class="next-steps">
  <section><span>Live granule ingest</span><h2>Listen for new STAC</h2><p>Producer publication events or bucket notifications for STAC JSON feed a queue.</p></section>
  <section><span>Incremental publication</span><h2>Commit small batches</h2><p>Fetch items and commit records to Iceberg as they arrive. Compact small files periodically.</p></section>
  <section><span>Full catalog representation</span><h2>Describe and discover</h2><p>Represent collections as well as items: schemas, extents, versions, and query entrypoints.</p></section>
</div>

<div class="takeaway"><a href="https://github.com/developmentseed/icestac">icestac</a>: a possible future Iceberg-backed STAC backend for continuously collected datasets like HLS.</div>

<!--
Next steps · about 1.5 minutes
- Goal: a near-real-time metadata archive, replacing monthly rebuilds with queued,
  micro-batched ingest. End-to-end latency depends on source publication, event
  delivery, batching, and commits; no instantaneous-availability promise.
- Prerequisite: access to producer granule-publication events or bucket
  notifications that identify available STAC JSON objects. We do not currently
  have or demonstrate that event source. A generic granule event must identify
  a fetchable STAC item, or the consumer needs retry/readiness handling.
- Queue consumers fetch producer STAC, convert items to rows, and commit batches
  to a persistent Iceberg table. Compact periodically to avoid many small files.
  This replaces today's static, rebuilt publication path; it is not just adding
  a queue in front of the existing publisher. Monthly files need not remain the
  unit of ingestion; the future physical layout is a separate design decision.
- Handle duplicate/retried/out-of-order events, idempotent updates, corrections,
  and deletions. Reconcile against the source to catch missing notifications;
  retain a backfill path. Events alone do not establish catalog completeness.
- A full catalog needs collection metadata and collection discovery as well as
  item tables, plus explicit freshness and publication guarantees.
- icestac is separate exploratory work and a candidate backend, not a deployed
  dependency here or a settled interoperable catalog standard. Evaluate its
  current capabilities and client support before choosing an implementation.
- Virtual arrays remain a consumer of the metadata, not the catalog's one fixed
  representation of the underlying observations.
-->

---
layout: cover
class: thank-you-slide
title: Thank you
---

<script setup>
import slidesQr from './public/images/slides-qr.svg'
import closingBackground from './public/images/landsat9-kangerdlugssuaq-greenland.jpg'
const slidesUrl = 'https://maap-project.github.io/hls-stac-geoparquet-archive/presentations/esdis-tech-spotlight-2026-11-17/'
</script>

<img :src="closingBackground" alt="" class="thank-you-background" />

<div class="thank-you-content">
<div>

# Thank you!

<a href="mailto:henry@developmentseed.org">henry@developmentseed.org</a>

</div>
<figure>
<a :href="slidesUrl"><img :src="slidesQr" alt="QR code linking to the published slides on the archive documentation site" /></a>
<figcaption><a :href="slidesUrl">slides</a></figcaption>
</figure>
</div>

<div class="thank-you-url"><a href="https://maap-project.github.io/hls-stac-geoparquet-archive/">https://maap-project.github.io/hls-stac-geoparquet-archive/</a></div>

<div class="thank-you-credit"><a href="https://unsplash.com/@usgs">USGS</a> on <a href="https://unsplash.com/photos/PgL1p8TBGNQ">Unsplash</a> · Kangerdlugssuaq Glacier, Greenland</div>

<!--
Closing · about 15 seconds, then 10–15 minutes of discussion
- The QR code encodes the final docs-site deck URL, not a local preview. It will
  point to this revised deck after publishing; no deployment is performed here.
- Leave this slide visible for questions. Next steps is the last content slide;
  everything after this is backup, used only for a concrete audience question.
- Discussion prompts, if useful:
  How stale can metadata be for your workflow?
  Which client would you want to use first?
  What would you need to trust the archive's coverage?
- Other topics: source notifications, producer-table maintenance, collection
  discovery, and whether a simple collection index meets immediate needs.
-->
