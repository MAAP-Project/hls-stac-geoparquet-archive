"""CMR query functions for HLS data."""

from collections.abc import AsyncGenerator
from typing import Any
from urllib.parse import ParseResult, urlparse

import httpx
from cmr import GranuleQuery

from hls_stac_parquet.constants import CLIENT_ID, HlsCollection


def create_hls_query(
    collection: HlsCollection,
    bounding_box: tuple[float, float, float, float] | None = None,
    temporal: tuple[str, str] | None = None,
) -> GranuleQuery:
    """Create a CMR granule query for HLS data.

    Args:
        collection_concept_ids: List of collection concept IDs. Defaults to HLS collections.
        bounding_box: Tuple of (west, south, east, north) coordinates
        temporal: Tuple of (start_date, end_date) in ISO format

    Returns:
        Configured GranuleQuery object
    """

    query = GranuleQuery().collection_concept_id(collection.concept_id)

    query.headers = {
        "client-id": CLIENT_ID,
    }

    if bounding_box:
        query = query.bounding_box(*bounding_box)

    if temporal:
        query = query.temporal(*temporal)

    return query.format("json")


async def get_cmr_results_async(
    query: GranuleQuery, page_size: int = 2000
) -> AsyncGenerator[dict[str, Any], None]:
    """Async generator that yields CMR granule results.

    Args:
        query: CMR GranuleQuery object
        page_size: Number of results per page (max 2000)

    Yields:
        Individual granule result dictionaries
    """
    page_size = min(max(1, page_size), 2000)
    url = query._build_url() + f"&page_size={page_size}"
    headers = dict(query.headers or {})

    async with httpx.AsyncClient(timeout=None) as client:
        while True:
            response = await client.get(url, headers=headers)
            response.raise_for_status()

            data = response.json()
            for entry in data["feed"]["entry"]:
                yield entry

            if not (cmr_search_after := response.headers.get("cmr-search-after")):
                break

            headers["cmr-search-after"] = cmr_search_after


def extract_stac_json_links(
    results: list[dict[str, Any]], protocol: str = "https"
) -> list[ParseResult]:
    """Extract STAC JSON links from CMR results.

    Args:
        results: List of CMR granule result dictionaries
        prefix: URL scheme prefix to filter by

    Returns:
        List of parsed STAC JSON URLs
    """
    stac_links = []
    for result in results:
        try:
            # Skip results without links or with malformed links
            if "links" not in result or not isinstance(result["links"], list):
                continue

            url = next(
                link["href"]
                for link in result["links"]
                if isinstance(link, dict)
                and "href" in link
                and link["href"].endswith("stac.json")
                and link["href"].startswith(protocol)
            )
            stac_links.append(urlparse(url))
        except (StopIteration, KeyError, TypeError):
            # Skip results without STAC JSON links or malformed data
            continue
    return stac_links


async def collect_cmr_results(
    query: GranuleQuery, page_size: int = 2000
) -> list[dict[str, Any]]:
    """Collect all CMR results into a list.

    Args:
        query: CMR GranuleQuery object
        page_size: Number of results per page

    Returns:
        List of all granule result dictionaries
    """
    results = []
    async for result in get_cmr_results_async(query, page_size):
        results.append(result)

    return results
