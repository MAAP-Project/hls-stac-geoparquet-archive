import asyncio
import json
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


def process_record(record: dict[str, Any]) -> None:
    """
    Process a single SQS record containing an SNS message.

    Args:
        record: SQS record containing SNS notification

    Raises:
        Exception: Any error during processing (will be caught by handler)
    """
    logger.info("Starting to process SQS record")

    # Extract SNS message from SQS record
    if "body" not in record:
        raise ValueError("Invalid SQS record: missing 'body' field")

    logger.info("Parsing SQS record body")
    # Handle both string (from SQS) and dict (from Step Functions) formats
    body = (
        record["body"]
        if isinstance(record["body"], dict)
        else json.loads(record["body"])
    )

    # Parse the SNS message
    if "Message" not in body:
        raise ValueError("Invalid SNS message: missing 'Message' field")

    logger.info("Parsing SNS message")
    # Handle both string (from SNS) and dict (from Step Functions) formats
    message = (
        body["Message"]
        if isinstance(body["Message"], dict)
        else json.loads(body["Message"])
    )
    logger.info(f"Processing message: {message}")

    # Extract and validate required parameters
    logger.info("Validating required parameters")
    collection_str = message.get("collection")
    if not collection_str:
        raise ValueError("Missing required parameter: 'collection'")
    logger.info(f"Collection: {collection_str}")

    date_str = message.get("date")
    if not date_str:
        raise ValueError("Missing required parameter: 'date'")
    logger.info(f"Date: {date_str}")

    # Get dest from message or environment variable
    dest = message.get("dest")
    if not dest:
        bucket_name = os.environ.get("BUCKET_NAME")
        if not bucket_name:
            raise ValueError(
                "Missing 'dest' parameter in message and BUCKET_NAME environment variable not set"
            )
        dest = f"s3://{bucket_name}"
        logger.info(f"Using default destination from BUCKET_NAME env var: {dest}")
    else:
        logger.info(f"Using destination from message: {dest}")

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
    bounding_box_list = message.get("bounding_box")
    bounding_box = None
    if bounding_box_list:
        if len(bounding_box_list) != 4:
            raise ValueError(
                f"Invalid bounding_box: expected 4 values, got {len(bounding_box_list)}"
            )
        bounding_box = tuple(bounding_box_list)
        logger.info(f"Bounding box: {bounding_box}")

    protocol = message.get("protocol", "s3")
    if protocol not in ["s3", "https"]:
        raise ValueError(f"Invalid protocol: {protocol}. Must be 's3' or 'https'")
    logger.info(f"Protocol: {protocol}")

    skip_existing = message.get("skip_existing", True)
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


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """
    Lambda handler for processing SQS messages containing SNS notifications.

    This handler supports partial batch failures. Failed records will be retried
    and eventually sent to the dead letter queue after maxReceiveCount attempts.

    Expected SNS message format (JSON):
    {
        "collection": "HLSL30" or "HLSS30",
        "date": "YYYY-MM-DD",
        "dest": "s3://bucket/path",  # optional, defaults to BUCKET_NAME env var
        "bounding_box": [min_lon, min_lat, max_lon, max_lat],  # optional
        "protocol": "s3" or "https",  # optional, default "s3"
        "skip_existing": true or false  # optional, default true
    }

    Environment Variables:
    - BUCKET_NAME: S3 bucket name (used if "dest" not provided in message)

    Returns:
        dict: Response with batchItemFailures for partial batch failure handling
    """
    logger.info(f"Lambda handler invoked with {len(event.get('Records', []))} records")

    if "Records" not in event:
        logger.error("Invalid event: missing 'Records' field")
        raise ValueError("Invalid event: missing 'Records' field")

    batch_item_failures = []

    # Process each SQS record individually
    for idx, record in enumerate(event["Records"], 1):
        message_id = record.get("messageId", "unknown")

        try:
            logger.info(
                f"[{idx}/{len(event['Records'])}] Processing record {message_id}"
            )
            process_record(record)
            logger.info(
                f"[{idx}/{len(event['Records'])}] Successfully processed record {message_id}"
            )

        except Exception as e:
            logger.error(
                f"[{idx}/{len(event['Records'])}] Failed to process record {message_id}: {str(e)}",
                exc_info=True,
            )
            # Add to batch item failures for retry
            batch_item_failures.append({"itemIdentifier": message_id})

    # Return batch item failures for partial batch failure handling
    # Records not in this list are considered successful and will be deleted from the queue
    # Records in this list will be retried and eventually sent to DLQ after maxReceiveCount
    logger.info(
        f"Batch processing complete. Successful: {len(event['Records']) - len(batch_item_failures)}, "
        f"Failed: {len(batch_item_failures)}"
    )

    return {"batchItemFailures": batch_item_failures}
