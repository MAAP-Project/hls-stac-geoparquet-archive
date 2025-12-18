"""Calculate previous month and generate dates array for Step Functions Map state."""

from calendar import monthrange
from datetime import UTC, datetime, timedelta

# Collection-specific origin dates (when data starts being available)
COLLECTION_ORIGIN_DATES = {
    "HLSL30": datetime(
        2013, 4, 11, tzinfo=UTC
    ),  # Landsat 8 launch + HLS processing start
    "HLSS30": datetime(
        2015, 11, 28, tzinfo=UTC
    ),  # Sentinel-2A launch + HLS processing start
}


def handler(event, context):
    """
    Calculate previous month and generate array of dates for cache-daily processing.

    This Lambda is invoked by Step Functions to prepare the data for the Map state
    that will process cache-daily operations for each day of the previous month.

    Input event format:
    {
        "collection": "HLSL30" or "HLSS30",
        "yearmonth": "2024-11-01",  # optional, specific month to process (YYYY-MM-DD)
        "time": "2024-12-15T10:00:00Z",  # optional, from EventBridge (ignored if yearmonth provided)
        "skip_existing": false  # optional, whether to skip existing files (default: true)
    }

    Output format:
    {
        "collection": "HLSL30",
        "yearMonth": "2024-11-01",
        "skip_existing": false,
        "dates": [
            {
                "date": "2024-11-01",
                "collection": "HLSL30",
                "skip_existing": false
            },
            {
                "date": "2024-11-02",
                "collection": "HLSL30",
                "skip_existing": false
            },
            ...
            {
                "date": "2024-11-30",
                "collection": "HLSL30",
                "skip_existing": false
            }
        ]
    }

    Note: cache-daily writes STAC links to link bucket (DEST env var)
          write-monthly reads STAC links from link bucket (SOURCE env var) and writes GeoParquet to dest bucket (DEST env var)
          dest and version are configured at deployment time via Lambda environment variables

    Returns:
        dict: Response with collection, yearMonth, and dates array
    """
    # Extract parameters
    collection = event.get("collection")
    skip_existing = event.get(
        "skip_existing", True
    )  # Default to True for backward compatibility

    # Determine which month to process
    if "yearmonth" in event:
        # Use explicitly provided month
        first_of_month = datetime.fromisoformat(
            event["yearmonth"].replace("Z", "+00:00")
        )
        first_of_month = first_of_month.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
    else:
        # Calculate previous month from current time or EventBridge time
        if "time" in event:
            # EventBridge provides time in ISO format with Z suffix
            now = datetime.fromisoformat(event["time"].replace("Z", "+00:00"))
        else:
            now = datetime.now()

        # Calculate first day of previous month
        first_of_this_month = now.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        last_day_of_last_month = first_of_this_month - timedelta(days=1)
        first_of_month = last_day_of_last_month.replace(day=1)

    # Get number of days in the target month
    year = first_of_month.year
    month = first_of_month.month
    days_in_month = monthrange(year, month)[1]

    # Get collection origin date to skip dates before data is available
    collection_origin = COLLECTION_ORIGIN_DATES[collection]

    # Generate array of date objects for Step Functions Map state
    # Note: cache-daily no longer needs dest (uses BUCKET_NAME env var)
    dates = []
    for day in range(1, days_in_month + 1):
        date_obj = first_of_month.replace(day=day)

        # Skip dates before collection origin date
        if date_obj < collection_origin:
            continue

        dates.append(
            {
                "date": date_obj.strftime("%Y-%m-%d"),
                "collection": collection,
                "skip_existing": skip_existing,
            }
        )

    return {
        "collection": collection,
        "yearMonth": first_of_month.strftime("%Y-%m-01"),
        "skip_existing": skip_existing,
        "dates": dates,
    }
