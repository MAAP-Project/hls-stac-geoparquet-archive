"""Lambda handler for generating a list of year-months for backfill operations."""

import json
import logging
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Collection-specific origin dates (when data starts being available)
COLLECTION_ORIGIN_DATES = {
    "HLSL30": datetime(2013, 4, 11),  # Landsat 8 launch + HLS processing start
    "HLSS30": datetime(2015, 11, 28),  # Sentinel-2A launch + HLS processing start
}


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """
    Lambda handler for generating a list of year-months for backfill.

    This function generates an array of month objects that can be used by a
    Step Functions Map state to process each month in parallel.

    Expected event format (JSON):
    {
        "collection": "HLSL30" or "HLSS30",
        "start_date": "YYYY-MM-DD",  # optional, defaults to collection origin date
        "end_date": "YYYY-MM-DD"     # optional, defaults to last complete month
    }

    Returns:
        dict: Response with array of month objects for Map state processing
        {
            "collection": "HLSL30",
            "months": [
                {"yearmonth": "2013-04-01", "collection": "HLSL30"},
                {"yearmonth": "2013-05-01", "collection": "HLSL30"},
                ...
            ]
        }

    Note: dest and version are configured at deployment time via Lambda environment variables.
    """
    logger.info(f"Month list generator invoked with event: {json.dumps(event)}")

    # Get and validate required parameters
    collection = event.get("collection")
    if not collection:
        raise ValueError("Missing required parameter: 'collection'")

    if collection not in COLLECTION_ORIGIN_DATES:
        raise ValueError(
            f"Invalid collection: {collection}. Must be 'HLSL30' or 'HLSS30'"
        )

    # Parse start_date (default to collection origin)
    start_date_str = event.get("start_date")
    if start_date_str:
        try:
            start_date = datetime.fromisoformat(start_date_str)
        except ValueError:
            raise ValueError(
                f"Invalid start_date format: {start_date_str}. Expected ISO format (YYYY-MM-DD)"
            )
    else:
        start_date = COLLECTION_ORIGIN_DATES[collection]
        logger.info(
            f"No start_date provided, using collection origin: {start_date.date()}"
        )

    # Normalize to first day of month
    start_date = start_date.replace(day=1)

    # Parse end_date (default to first day of current month)
    end_date_str = event.get("end_date")
    if end_date_str:
        try:
            end_date = datetime.fromisoformat(end_date_str)
        except ValueError:
            raise ValueError(
                f"Invalid end_date format: {end_date_str}. Expected ISO format (YYYY-MM-DD)"
            )
    else:
        # Default to last complete month (previous month)
        now = datetime.now()
        first_of_this_month = now.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        last_day_of_last_month = first_of_this_month - timedelta(days=1)
        end_date = last_day_of_last_month.replace(day=1)
        logger.info(
            f"No end_date provided, using last complete month: {end_date.date()}"
        )

    # Normalize to first day of month
    end_date = end_date.replace(day=1)

    # Validate date range
    if start_date > end_date:
        raise ValueError(
            f"start_date ({start_date.date()}) must be before or equal to end_date ({end_date.date()})"
        )

    # Generate list of months
    months = []
    current_date = start_date

    while current_date <= end_date:
        month_obj = {
            "yearmonth": current_date.strftime("%Y-%m-01"),
            "collection": collection,
        }

        months.append(month_obj)

        # Move to next month using calendar arithmetic
        year = current_date.year
        month = current_date.month
        if month == 12:
            current_date = current_date.replace(year=year + 1, month=1)
        else:
            current_date = current_date.replace(month=month + 1)

    num_months = len(months)
    logger.info(
        f"Generated {num_months} months for {collection} from "
        f"{start_date.date()} to {end_date.date()}"
    )

    response = {
        "collection": collection,
        "months": months,
        "num_months": num_months,
        "start_date": start_date.strftime("%Y-%m-01"),
        "end_date": end_date.strftime("%Y-%m-01"),
    }

    return response
