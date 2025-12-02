"""Lambda handler for write-monthly-stac-geoparquet operations."""

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Any

from hls_stac_parquet import __version__
from hls_stac_parquet.cmr_api import HlsCollection
from hls_stac_parquet.write import write_monthly_stac_geoparquet

# Configure logger for Lambda environment
logger = logging.getLogger(__name__)
# Set level directly on logger since Lambda pre-configures root logger
logger.setLevel(logging.INFO)

# Also set level for imported module loggers to see their output
logging.getLogger("hls_stac_parquet").setLevel(logging.INFO)


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """
    Lambda handler for write-monthly operations.

    This handler invokes the write_monthly_stac_geoparquet function to create
    monthly GeoParquet files from cached STAC JSON links.

    Expected event format (JSON):
    {
        "collection": "HLSL30" or "HLSS30",
        "yearmonth": "YYYY-MM-DD",  # Day is ignored, only year and month used
        "dest": "s3://bucket/path",  # optional, defaults to BUCKET_NAME env var
        "version": "0.2.0",  # optional, defaults to package version
        "require_complete_links": true,  # optional, default true
        "skip_existing": true,  # optional, default true
        "batch_size": 1000  # optional, default 1000
    }

    Environment Variables:
    - BUCKET_NAME: S3 bucket name (used if "dest" not provided in event)

    Returns:
        dict: Response with status, collection, yearmonth, and dest
    """
    logger.info(f"Write-monthly Lambda invoked with event: {json.dumps(event)}")

    try:
        # Extract and validate required parameters
        logger.info("Validating required parameters")
        collection_str = event.get("collection")
        if not collection_str:
            raise ValueError("Missing required parameter: 'collection'")
        logger.info(f"Collection: {collection_str}")

        yearmonth_str = event.get("yearmonth")
        if not yearmonth_str:
            raise ValueError("Missing required parameter: 'yearmonth'")
        logger.info(f"Year-month: {yearmonth_str}")

        # Get dest from event or environment variable
        dest = event.get("dest")
        if not dest:
            bucket_name = os.environ.get("BUCKET_NAME")
            if not bucket_name:
                raise ValueError(
                    "Missing 'dest' parameter in event and BUCKET_NAME environment variable not set"
                )
            dest = f"s3://{bucket_name}"
            logger.info(f"Using default destination from BUCKET_NAME env var: {dest}")
        else:
            logger.info(f"Using destination from event: {dest}")

        # Convert collection string to enum
        logger.info("Converting collection string to enum")
        try:
            collection = HlsCollection[collection_str]
            logger.info(f"Collection enum: {collection}")
        except KeyError:
            raise ValueError(
                f"Invalid collection: {collection_str}. Must be 'HLSL30' or 'HLSS30'"
            )

        # Parse yearmonth
        logger.info("Parsing yearmonth")
        try:
            yearmonth = datetime.fromisoformat(yearmonth_str)
            logger.info(f"Parsed yearmonth: {yearmonth.year}-{yearmonth.month:02d}")
        except ValueError:
            raise ValueError(
                f"Invalid yearmonth format: {yearmonth_str}. Expected ISO format (YYYY-MM-DD)"
            )

        # Extract optional parameters
        logger.info("Processing optional parameters")
        version = event.get("version", __version__)
        logger.info(f"Version: {version}")

        require_complete_links = event.get("require_complete_links", True)
        logger.info(f"Require complete links: {require_complete_links}")

        skip_existing = event.get("skip_existing", True)
        logger.info(f"Skip existing: {skip_existing}")

        batch_size = event.get("batch_size", 1000)
        logger.info(f"Batch size: {batch_size}")

        # Execute the async write-monthly function
        logger.info(
            f"Writing monthly GeoParquet for {collection.value} {yearmonth.year}-{yearmonth.month:02d} to {dest}"
        )
        logger.info("Starting async write operation...")

        asyncio.run(
            write_monthly_stac_geoparquet(
                collection=collection,
                yearmonth=yearmonth,
                dest=dest,
                version=version,
                require_complete_links=require_complete_links,
                skip_existing=skip_existing,
                batch_size=batch_size,
            )
        )

        logger.info("Successfully completed write-monthly operation")

        # Return success response
        return {
            "statusCode": 200,
            "status": "success",
            "collection": collection.value,
            "yearmonth": yearmonth_str,
            "dest": dest,
        }

    except Exception as e:
        logger.error(
            f"Write-monthly operation failed: {str(e)}",
            exc_info=True,
        )
        # Return error response
        return {
            "statusCode": 500,
            "status": "failed",
            "error": str(e),
            "collection": event.get("collection"),
            "yearmonth": event.get("yearmonth"),
        }
