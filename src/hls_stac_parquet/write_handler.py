"""Lambda handler for write-monthly-stac-geoparquet operations."""

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Any

from hls_stac_parquet.cmr_api import HlsCollection
from hls_stac_parquet.write import write_monthly_stac_geoparquet

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
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
        "require_complete_links": true,  # optional, default true
        "skip_existing": true,  # optional, default true
        "batch_size": 1000  # optional, default 1000
    }

    Environment Variables:
    - SOURCE: S3 URI for reading STAC JSON links (required, e.g., s3://bucket-name)
    - DEST: S3 URI for writing GeoParquet files (required, e.g., s3://bucket-name)
    - VERSION: Version string for pipeline output (required)

    Returns:
        dict: Response with status, collection, yearmonth, source, and dest
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

        # Get source from SOURCE environment variable
        source = os.environ.get("SOURCE")
        if not source:
            raise ValueError("SOURCE environment variable not set")
        logger.info(f"Using SOURCE for reading STAC JSON links: {source}")

        # Get dest from DEST environment variable
        dest = os.environ.get("DEST")
        if not dest:
            raise ValueError("DEST environment variable not set")
        logger.info(f"Using DEST for writing GeoParquet files: {dest}")

        # Get version from environment variable
        version = os.environ.get("VERSION")
        if not version:
            raise ValueError("VERSION environment variable not set")
        logger.info(f"Pipeline version from environment: {version}")

        # Convert collection string to enum
        logger.info("Converting collection string to enum")
        try:
            collection = HlsCollection[collection_str]
            logger.info(f"Collection enum: {collection}")
        except KeyError as e:
            raise ValueError(
                f"Invalid collection: {collection_str}. Must be 'HLSL30' or 'HLSS30'"
            ) from e

        # Parse yearmonth
        logger.info("Parsing yearmonth")
        try:
            yearmonth = datetime.fromisoformat(yearmonth_str)
            logger.info(f"Parsed yearmonth: {yearmonth.year}-{yearmonth.month:02d}")
        except ValueError as e:
            raise ValueError(
                f"Invalid yearmonth format: {yearmonth_str}. Expected ISO format (YYYY-MM-DD)"
            ) from e

        # Extract optional parameters
        logger.info("Processing optional parameters")
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
                source=source,
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
            "source": source,
            "dest": dest,
        }

    except Exception as e:
        logger.error(
            f"Write-monthly operation failed: {str(e)}",
            exc_info=True,
        )
        # Re-raise the exception so Step Functions sees it as a failure
        # This allows Step Functions retry logic to work properly
        raise
