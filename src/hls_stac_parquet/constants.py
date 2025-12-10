from datetime import datetime
from enum import StrEnum

CLIENT_ID = "nasa-maap-hls-stac-geoparquet; contact henry@developmentseed.org"
TEST = "test"
LINK_PATH_PREFIX = "links/{collection_id}/{year}/{month:02d}"
LINK_PATH_FORMAT = LINK_PATH_PREFIX + "/{year}-{month:02d}-{day:02d}.json"
PARQUET_PATH_FORMAT = "{version}/{collection_id}/year={year}/month={month}/{collection_id}-{year}-{month}.parquet"

# Collection-specific origin dates (when data starts being available)
COLLECTION_ORIGIN_DATES = {
    "HLSL30": datetime(2013, 4, 11),  # Landsat 8 launch + HLS processing start
    "HLSS30": datetime(2015, 11, 28),  # Sentinel-2A launch + HLS processing start
}

COLLECTION_CONCEPT_IDS = {
    "HLSL30": "C2021957657-LPCLOUD",
    "HLSS30": "C2021957295-LPCLOUD",
}


class HlsCollection(StrEnum):
    """HLS collection identifiers with associated concept IDs."""

    HLSL30 = "HLSL30"
    HLSS30 = "HLSS30"

    @property
    def concept_id(self) -> str:
        """Return the CMR concept ID for this collection."""
        return COLLECTION_CONCEPT_IDS[self.value]

    @property
    def collection_id(self) -> str:
        """Return the collection ID with _2.0 appended."""
        return f"{self.value}_2.0"

    @property
    def origin_date(self) -> datetime:
        """Return the origin date for this collection."""
        return COLLECTION_ORIGIN_DATES[self.value]
