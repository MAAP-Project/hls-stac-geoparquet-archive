"""Calculate previous month and generate dates array for Step Functions Map state."""

from calendar import monthrange
from datetime import datetime, timedelta


def handler(event, context):
    """
    Calculate previous month and generate array of dates for cache-daily processing.

    This Lambda is invoked by Step Functions to prepare the data for the Map state
    that will process cache-daily operations for each day of the previous month.

    Input event format:
    {
        "collection": "HLSL30" or "HLSS30",
        "dest": "s3://bucket/path",  # optional
        "yearmonth": "2024-11-01",  # optional, specific month to process (YYYY-MM-DD)
        "version": "v0.1.0",  # optional, version string for output path
        "time": "2024-12-15T10:00:00Z"  # optional, from EventBridge (ignored if yearmonth provided)
    }

    Output format:
    {
        "collection": "HLSL30",
        "yearMonth": "2024-11-01",
        "dest": "s3://bucket",
        "version": "v0.1.0",  # optional, only included if provided in input
        "dates": [
            {
                "date": "2024-11-01",
                "collection": "HLSL30",
                "dest": "s3://bucket",
                "skip_existing": true
            },
            {
                "date": "2024-11-02",
                "collection": "HLSL30",
                "dest": "s3://bucket",
                "skip_existing": true
            },
            ...
            {
                "date": "2024-11-30",
                "collection": "HLSL30",
                "dest": "s3://bucket",
                "skip_existing": true
            }
        ]
    }

    Returns:
        dict: Response with collection, yearMonth, dest, and dates array
    """
    # Extract parameters
    collection = event.get("collection")
    dest = event.get("dest")
    version = event.get("version")  # optional version parameter

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

    # Generate array of date objects for Step Functions Map state
    dates = []
    for day in range(1, days_in_month + 1):
        date_obj = first_of_month.replace(day=day)
        dates.append(
            {
                "date": date_obj.strftime("%Y-%m-%d"),
                "collection": collection,
                "dest": dest,
                "skip_existing": True,  # Always skip existing for automated runs
            }
        )

    # Build response for Step Functions
    response = {
        "collection": collection,
        "yearMonth": first_of_month.strftime("%Y-%m-01"),
        "dest": dest,
        "dates": dates,
    }

    # Include version if provided
    if version:
        response["version"] = version

    return response
