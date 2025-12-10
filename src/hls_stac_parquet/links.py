"""Functions for collecting and caching STAC JSON links."""

import json
import logging
from datetime import datetime, timedelta
from typing import Annotated, List, Literal
from urllib.parse import ParseResult

import obstore
import typer
from obstore.store import ObjectStore, from_url

from hls_stac_parquet.cmr_api import (
    HlsCollection,
    collect_cmr_results,
    create_hls_query,
    extract_stac_json_links,
)
from hls_stac_parquet.constants import LINK_PATH_FORMAT

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

logger = logging.getLogger(__name__)


async def _check_exists(store, path) -> bool:
    try:
        _ = await obstore.head_async(store, path)
        return True
    except FileNotFoundError:
        return False


async def collect_stac_json_links(
    collection: HlsCollection,
    bounding_box: tuple[float, float, float, float] | None = None,
    temporal: tuple[str, str] | None = None,
    protocol: Literal["s3", "https"] = "https",
) -> List[ParseResult]:
    query = create_hls_query(
        collection=collection,
        bounding_box=bounding_box,
        temporal=temporal,
    )

    cmr_results = await collect_cmr_results(query)

    return extract_stac_json_links(cmr_results, protocol=protocol)


async def write_stac_links(
    stac_links: List[ParseResult],
    store: ObjectStore,
    path: str,
) -> None:
    links = json.dumps([link.geturl() for link in stac_links]).encode()
    await obstore.put_async(store, path, links)


async def cache_daily_stac_json_links(
    collection: Annotated[
        HlsCollection, typer.Argument(help="HLS collection to query (HLSL30 or HLSS30)")
    ],
    date: Annotated[datetime, typer.Argument(help="Date to query for STAC items")],
    dest: Annotated[
        str,
        typer.Argument(
            help="Destination URL for storing cached links (e.g., s3://bucket/path)"
        ),
    ],
    bounding_box: Annotated[
        tuple[float, float, float, float] | None,
        typer.Option(
            help="Spatial bounding box as (min_lon, min_lat, max_lon, max_lat)"
        ),
    ] = None,
    protocol: Annotated[
        Literal["s3", "https"], typer.Option(help="Protocol to use for STAC JSON links")
    ] = "https",
    skip_existing: Annotated[
        bool, typer.Option(help="Skip processing if output file already exists")
    ] = False,
) -> None:
    """
    Cache daily STAC JSON links from CMR to object storage.

    Queries CMR for HLS STAC items on a specific date and writes the
    STAC JSON links to object storage for later retrieval.
    """
    store = from_url(dest)
    out_path = LINK_PATH_FORMAT.format(
        collection_id=collection.collection_id,
        year=date.year,
        month=date.month,
        day=date.day,
    )

    if skip_existing:
        if await _check_exists(store, out_path):
            logger.info(f"{dest}/{out_path} already exists ... skipping")
            return

    start_datetime = datetime(year=date.year, month=date.month, day=date.day)
    end_datetime = start_datetime + timedelta(days=1) - timedelta(seconds=1)

    stac_links = await collect_stac_json_links(
        collection=collection,
        bounding_box=bounding_box,
        temporal=(start_datetime.isoformat(), end_datetime.isoformat()),
        protocol=protocol,
    )

    await write_stac_links(
        stac_links=stac_links,
        store=store,
        path=out_path,
    )
