"""Helpers for publishing static Iceberg metadata."""

from dataclasses import dataclass
from pathlib import Path
from shutil import copyfile

import pyarrow.parquet as pq
from pyiceberg.catalog import load_in_memory


@dataclass(frozen=True)
class IcebergPublishResult:
    """Locations written by a static Iceberg metadata publication."""

    metadata_location: str
    latest_metadata_path: Path
    metadata_pointer_path: Path


def publish_local_static_iceberg_table(
    parquet_files: list[Path], table_location: Path, table_name: str = "hls"
) -> IcebergPublishResult:
    """Publish local parquet files as a static Iceberg table for compatibility tests."""
    if not parquet_files:
        raise ValueError("at least one parquet file is required")

    missing_files = [path for path in parquet_files if not path.exists()]
    if missing_files:
        raise FileNotFoundError(missing_files[0])

    table_location.mkdir(parents=True, exist_ok=True)
    schema = pq.read_schema(parquet_files[0])

    catalog = load_in_memory(
        "static",
        {"warehouse": (table_location.parent / ".pyiceberg-catalog").as_uri()},
    )
    catalog.create_namespace("default")
    table = catalog.create_table(
        f"default.{table_name}", schema, location=table_location.as_uri()
    )
    table.add_files([str(path) for path in parquet_files])
    table = table.refresh()

    metadata_path = Path(table.metadata_location.removeprefix("file://"))
    latest_metadata_path = table_location / "metadata" / "latest.metadata.json"
    metadata_pointer_path = table_location / "metadata" / "latest.metadata-location.txt"

    copyfile(metadata_path, latest_metadata_path)
    metadata_pointer_path.write_text(table.metadata_location)

    return IcebergPublishResult(
        metadata_location=table.metadata_location,
        latest_metadata_path=latest_metadata_path,
        metadata_pointer_path=metadata_pointer_path,
    )
