import asyncio
import logging
import os
from datetime import datetime
from typing import Any

from hls_stac_parquet.cmr_api import HlsCollection
from hls_stac_parquet.links import cache_daily_stac_json_links

# Configure logger for Lambda environment
logger = logging.getLogger(__name__)
# Set level directly on logger since Lambda pre-configures root logger
logger.setLevel(logging.INFO)

# Also set level for imported module loggers to see their output
logging.getLogger("hls_stac_parquet").setLevel(logging.INFO)


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """
    Lambda handler for caching daily STAC JSON links.

    Expected event format:
    {
        "collection": "HLSL30" or "HLSS30",
        "date": "YYYY-MM-DD",
        "bounding_box": [min_lon, min_lat, max_lon, max_lat],  # optional
        "protocol": "s3" or "https",  # optional, default "s3"
        "skip_existing": true or false  # optional, default true
    }

    Environment Variables:
    - BUCKET_NAME: S3 bucket name for storing STAC JSON links (required)

    Returns:
        dict: Response with success status and message
    """
    logger.info("Lambda handler invoked")
    logger.info(f"Event: {event}")

    # Extract and validate required parameters
    logger.info("Validating required parameters")
    collection_str = event.get("collection")
    if not collection_str:
        raise ValueError("Missing required parameter: 'collection'")
    logger.info(f"Collection: {collection_str}")

    date_str = event.get("date")
    if not date_str:
        raise ValueError("Missing required parameter: 'date'")
    logger.info(f"Date: {date_str}")

    # Get dest from environment variable only (always use stack bucket)
    bucket_name = os.environ.get("BUCKET_NAME")
    if not bucket_name:
        raise ValueError("BUCKET_NAME environment variable not set")
    dest = f"s3://{bucket_name}"
    logger.info(f"Using stack bucket for STAC JSON links: {dest}")

    # Convert collection string to enum
    logger.info("Converting collection string to enum")
    try:
        collection = HlsCollection[collection_str]
        logger.info(f"Collection enum: {collection}")
    except KeyError:
        raise ValueError(
            f"Invalid collection: {collection_str}. Must be 'HLSL30' or 'HLSS30'"
        )

    # Parse date
    logger.info("Parsing date")
    try:
        date = datetime.fromisoformat(date_str)
        logger.info(f"Parsed date: {date.date()}")
    except ValueError:
        raise ValueError(
            f"Invalid date format: {date_str}. Expected ISO format (YYYY-MM-DD)"
        )

    # Extract optional parameters
    logger.info("Processing optional parameters")
    bounding_box_list = event.get("bounding_box")
    bounding_box = None
    if bounding_box_list:
        if len(bounding_box_list) != 4:
            raise ValueError(
                f"Invalid bounding_box: expected 4 values, got {len(bounding_box_list)}"
            )
        bounding_box = tuple(bounding_box_list)
        logger.info(f"Bounding box: {bounding_box}")

    protocol = event.get("protocol", "s3")
    if protocol not in ["s3", "https"]:
        raise ValueError(f"Invalid protocol: {protocol}. Must be 's3' or 'https'")
    logger.info(f"Protocol: {protocol}")

    skip_existing = event.get("skip_existing", True)
    logger.info(f"Skip existing: {skip_existing}")

    # Execute the async cache function
    logger.info(f"Caching STAC links for {collection.value} on {date.date()} to {dest}")
    logger.info("Starting async cache operation...")

    asyncio.run(
        cache_daily_stac_json_links(
            collection=collection,
            date=date,
            dest=dest,
            bounding_box=bounding_box,
            protocol=protocol,
            skip_existing=skip_existing,
        )
    )

    logger.info("Successfully completed cache operation")

    return {
        "statusCode": 200,
        "body": f"Successfully cached STAC links for {collection.value} on {date.date()}",
    }
