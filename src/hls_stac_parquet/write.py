"""Functions for writing monthly STAC GeoParquet files."""

import asyncio
import json
import logging
import re
import urllib.parse
import warnings
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from typing import Annotated, Any
from urllib.parse import ParseResult

import obstore
import typer
from hilbertcurve.hilbertcurve import HilbertCurve
from mgrs import MGRS
from obstore.store import from_url
from rustac.geoparquet import geoparquet_writer
from rustac.rustac import GeoparquetWriter

from hls_stac_parquet import __version__
from hls_stac_parquet.cmr_api import HlsCollection
from hls_stac_parquet.constants import (
    LINK_PATH_FORMAT,
    LINK_PATH_PREFIX,
    PARQUET_PATH_FORMAT,
)
from hls_stac_parquet.fetch import fetch_stac_items

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

logging.getLogger("stac_io").setLevel("WARN")

logger = logging.getLogger(__name__)

# Suppress warning about store reconstruction across modules (expected behavior)
warnings.filterwarnings(
    "ignore",
    message="Successfully reconstructed a store defined in another Python module",
    category=RuntimeWarning,
)

# Initialize MGRS converter and Hilbert curve (14 bits = 16384x16384 grid)
_mgrs_converter = MGRS()
_hilbert_curve = HilbertCurve(14, 2)

# Regex pattern to extract MGRS tile ID from HLS STAC URLs
# Pattern: HLS.{SENSOR}.T{MGRS_TILE}.{DATE}.v{VERSION}
_MGRS_PATTERN = re.compile(r"\.T([0-9]{2}[A-Z]{3})\.")


def extract_mgrs_from_url(url: str) -> str | None:
    """
    Extract MGRS tile ID from HLS STAC JSON URL.

    Example URL:
    https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-public/HLSS30.020/
    HLS.S30.T60WWV.2025275T234641.v2.0/HLS.S30.T60WWV.2025275T234641.v2.0_stac.json

    Returns: "60WWV" or None if pattern not found
    """
    match = _MGRS_PATTERN.search(url)
    return match.group(1) if match else None


def mgrs_to_hilbert_index(mgrs_tile: str) -> int:
    """
    Convert MGRS tile ID to Hilbert curve index for spatial sorting.

    Args:
        mgrs_tile: MGRS tile identifier (e.g., "60WWV")

    Returns:
        Hilbert curve index (integer) for spatial ordering
    """
    try:
        # Convert MGRS tile to lat/lon coordinates
        # Use center of tile (60000m, 60000m offset in a ~110km tile)
        lat, lon = _mgrs_converter.toLatLon(mgrs_tile)

        # Normalize coordinates to grid space [0, 16384)
        # Longitude: -180 to +180 -> 0 to 16384
        # Latitude: -90 to +90 -> 0 to 16384
        x = int((lon + 180) / 360 * 16384)
        y = int((lat + 90) / 180 * 16384)

        # Clamp to valid range
        x = max(0, min(16383, x))
        y = max(0, min(16383, y))

        # Calculate Hilbert curve distance
        return _hilbert_curve.distance_from_point([x, y])
    except Exception as e:
        logger.warning(f"Failed to convert MGRS tile {mgrs_tile} to Hilbert index: {e}")
        # Return a large number to sort errors to the end
        return 2**28


async def _check_exists(store, path) -> bool:
    try:
        _ = await obstore.head_async(store, path)
        return True
    except FileNotFoundError:
        return False


def _check_complete(
    year: int, month: int, collection: HlsCollection, actual_links: list[str]
) -> None:
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    last_date_in_month = datetime(year=next_year, month=next_month, day=1) - timedelta(
        days=1
    )

    # Handle partial first month if this is the origin month
    origin_date = collection.origin_date
    first_day = 1
    if year == origin_date.year and month == origin_date.month:
        first_day = origin_date.day
        logger.info(
            f"Origin month detected: expecting links starting from day {first_day}"
        )

    expected_links = [
        LINK_PATH_FORMAT.format(
            collection_id=collection.collection_id,
            year=year,
            month=month,
            day=day,
        )
        for day in range(first_day, last_date_in_month.day + 1)
    ]

    if not set(expected_links) == set(actual_links):
        raise ValueError(
            f"expected these links: \n{'\n'.join(expected_links)}\n",
            f"found these links:\n{'\n'.join(actual_links)}",
        )


def _hilbert_sort_key(url: str) -> int:
    """Extract MGRS tile from URL and convert to Hilbert index for sorting."""
    mgrs_tile = extract_mgrs_from_url(url)
    if mgrs_tile:
        return mgrs_to_hilbert_index(mgrs_tile)
    else:
        # If we can't extract MGRS, sort to end
        logger.warning(f"Could not extract MGRS tile from URL: {url}")
        return 2**28


async def _producer(
    batches: AsyncGenerator[tuple[list[dict[str, Any]], list[ParseResult]], None],
    queue: asyncio.Queue[tuple[list[dict[str, Any]], list[ParseResult]] | None],
) -> None:
    """Fetch batches from async generator and enqueue them for writing."""
    try:
        async for batch_items, batch_failed_links in batches:
            await queue.put((batch_items, batch_failed_links))
    finally:
        # Signal completion
        await queue.put(None)


async def _consumer(
    queue: asyncio.Queue[tuple[list[dict[str, Any]], list[ParseResult]] | None],
    writer: GeoparquetWriter,
    first_batch_size: int,
    collection_id: str,
) -> tuple[int, list[ParseResult]]:
    """Dequeue batches and write them to parquet."""
    total_items = first_batch_size
    failed_links: list[ParseResult] = []

    while True:
        chunk = await queue.get()

        # Check for sentinel
        if chunk is None:
            break

        batch_items, batch_failed_links = chunk
        failed_links.extend(batch_failed_links)

        await writer.write(batch_items)
        total_items += len(batch_items)
        logger.info(
            f"{collection_id}: wrote batch of {len(batch_items)} items (total: {total_items})"
        )

        queue.task_done()

    return total_items, failed_links


async def write_monthly_stac_geoparquet(
    collection: Annotated[
        HlsCollection,
        typer.Argument(help="HLS collection to process (HLSL30 or HLSS30)"),
    ],
    yearmonth: Annotated[
        datetime,
        typer.Argument(help="Year and month to process (YYYY-MM-DD, day is ignored)"),
    ],
    dest: Annotated[
        str,
        typer.Argument(
            help="Destination URL for writing GeoParquet file (e.g., s3://bucket/path)"
        ),
    ],
    version: Annotated[
        str, typer.Option(help="Version string for output file path")
    ] = __version__,
    require_complete_links: Annotated[
        bool,
        typer.Option(
            help="Require all daily link files for the month to exist before processing"
        ),
    ] = False,
    skip_existing: Annotated[
        bool,
        typer.Option(help="Skip processing if output GeoParquet file already exists"),
    ] = False,
    batch_size: Annotated[
        int,
        typer.Argument(help="batch size for writing STAC items to parquet files"),
    ] = 1000,
) -> None:
    """
    Write monthly STAC items to GeoParquet format.

    Collects cached STAC JSON links for a given month, fetches all STAC items,
    and writes them to a GeoParquet file in object storage.
    """
    store = from_url(dest)

    year = yearmonth.year
    month = yearmonth.month

    out_path = PARQUET_PATH_FORMAT.format(
        version=version,
        collection_id=collection.collection_id,
        year=str(year),
        month=str(month),
    )

    if skip_existing:
        if await _check_exists(store, out_path):
            logger.info(f"{out_path} found in {dest}... skipping")
            return

    stac_json_links = []
    stream = obstore.list(
        store,
        prefix=LINK_PATH_PREFIX.format(
            collection_id=collection.collection_id,
            year=year,
            month=month,
        ),
    )

    actual_links = []
    async for list_result in stream:
        for result in list_result:
            actual_links.append(result["path"])
            resp = await obstore.get_async(store, result["path"])
            buffer = await resp.bytes_async()
            links = json.loads(bytes(buffer).decode())
            stac_json_links.extend(links)

    logger.info(f"{collection.collection_id}: found {len(stac_json_links)} links")

    if require_complete_links:
        _check_complete(
            year=year, month=month, collection=collection, actual_links=actual_links
        )

    # Sort links by Hilbert curve for optimal spatial ordering
    logger.info(
        f"{collection.collection_id}: sorting {len(stac_json_links)} links by spatial order (Hilbert curve)"
    )

    stac_json_links.sort(key=_hilbert_sort_key)

    logger.info(f"{collection.collection_id}: loading stac items in batches")

    # Create async generator for batches
    batches = fetch_stac_items(
        [urllib.parse.urlparse(link) for link in stac_json_links],
        collection_id=collection.collection_id,
        max_concurrent=50,
        batch_size=batch_size,
    )

    # Get first batch to initialize writer
    first_batch, all_failed_links = await anext(batches)

    queue: asyncio.Queue[tuple[list[dict[str, Any]], list[ParseResult]] | None] = (
        asyncio.Queue(maxsize=5)
    )

    # Initialize writer with first batch and run producer/consumer concurrently
    async with geoparquet_writer(first_batch, out_path, store=store) as writer:
        logger.info(
            f"{collection.collection_id}: initialized writer with {len(first_batch)} items"
        )

        producer_task = asyncio.create_task(
            _producer(
                batches,
                queue,
            )
        )
        total_items_written, consumer_failed_links = await _consumer(
            queue, writer, len(first_batch), collection.collection_id
        )
        await producer_task

        all_failed_links.extend(consumer_failed_links)

    if all_failed_links:
        logger.warning(f"failed to retrieve {len(all_failed_links)} items")

    logger.info(f"successfully wrote {total_items_written} items to {out_path}")
