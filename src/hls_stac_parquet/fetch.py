"""Async STAC item fetching functions."""

import asyncio
import json
from typing import Any, AsyncGenerator, Dict, List
from urllib.parse import ParseResult

import obstore as obs
from obstore.auth.earthdata import NasaEarthdataAsyncCredentialProvider
from obstore.store import from_url


async def fetch_stac_items(
    stac_links: List[ParseResult],
    collection_id: str,
    max_concurrent: int = 50,
    batch_size: int = 1000,
) -> AsyncGenerator[tuple[list[dict[str, Any]], list[ParseResult]]]:
    """Fetch STAC items in batches and yield them as they become available.

    Args:
        stac_links: List of parsed STAC JSON URLs
        max_concurrent: Maximum number of concurrent requests
        batch_size: Number of items per batch to yield

    Yields:
        Tuple of (batch_items, failed_links) where batch_items is a list of STAC items
        and failed_links is a list of ParseResults that failed in this batch
    """
    if not stac_links:
        return

    # Group by netloc to create stores efficiently
    # Keep track of credential providers so we can close them
    stores_by_netloc = {}
    credential_providers = []

    for link in stac_links:
        netloc = link.netloc
        if netloc not in stores_by_netloc:
            store_kwargs = {}
            if link.scheme == "s3":
                cp = NasaEarthdataAsyncCredentialProvider(
                    credentials_url="https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials"
                )
                credential_providers.append(cp)
                store_kwargs["credential_provider"] = cp

            stores_by_netloc[netloc] = from_url(
                f"{link.scheme}://{netloc}", **store_kwargs
            )

    semaphore = asyncio.Semaphore(max_concurrent)

    async def fetch_with_error_handling(
        link: ParseResult,
    ) -> tuple[Dict[str, Any] | None, ParseResult | None]:
        async with semaphore:
            try:
                store = stores_by_netloc[link.netloc]
                item_data = await obs.get_async(store, link.path)
                item_bytes = await item_data.bytes_async()
                item = json.loads(item_bytes.to_bytes().decode("utf-8"))
                item["collection"] = collection_id

                return item, None
            except Exception as e:
                print(f"Failed to fetch {link.geturl()}: {e}")
                return None, link

    try:
        # Execute fetches concurrently and process results as they complete
        tasks = [fetch_with_error_handling(link) for link in stac_links]

        batch_items = []
        batch_failed_links = []
        completed_count = 0

        # Use as_completed to process results as they arrive

        for coro in asyncio.as_completed(tasks):
            item, failed_link = await coro
            completed_count += 1

            if item is not None:
                batch_items.append(item)
            if failed_link is not None:
                batch_failed_links.append(failed_link)

            # Yield when batch is full or all tasks are complete
            if len(batch_items) >= batch_size or completed_count == len(tasks):
                if batch_items:  # Only yield if we have items
                    yield batch_items, batch_failed_links
                    batch_items = []
                    batch_failed_links = []

    finally:
        # Close all credential providers
        for cp in credential_providers:
            await cp.close()
