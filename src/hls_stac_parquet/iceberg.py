"""Helpers for publishing static Iceberg metadata."""

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow.parquet as pq
from pyiceberg.catalog import load_in_memory
from pyiceberg.io import pyarrow as pyiceberg_pyarrow
from pyiceberg.io import load_file_io

from hls_stac_parquet.constants import (
    ICEBERG_TABLE_PATH_FORMAT,
    PARQUET_PATH_FORMAT,
    HlsCollection,
)
from hls_stac_parquet.storage import pyiceberg_s3_properties, store_from_url

_LIST_ITEM_PATH_PATCHED = False


@dataclass(frozen=True)
class IcebergPublishResult:
    """Locations written by a static Iceberg metadata publication."""

    metadata_location: str
    latest_metadata_location: str


def publish_static_iceberg_table(
    *, collection: HlsCollection, year: int, month: int, dest: str, version: str
) -> IcebergPublishResult:
    """Publish one collection/month parquet object into static Iceberg metadata."""
    parquet_location = _join_uri(
        dest,
        PARQUET_PATH_FORMAT.format(
            version=version,
            collection_id=collection.collection_id,
            year=year,
            month=month,
        ),
    )
    table_location = _join_uri(
        dest,
        ICEBERG_TABLE_PATH_FORMAT.format(
            version=version, collection_id=collection.collection_id
        ),
    )
    parquet_files = _collection_parquet_files(dest, version, collection.collection_id)
    if parquet_location not in parquet_files:
        raise FileNotFoundError(parquet_location)

    return _publish_static_iceberg_table(
        parquet_files, table_location, collection.collection_id.replace(".", "_")
    )


def _publish_static_iceberg_table(
    parquet_files: list[str], table_location: str, table_name: str
) -> IcebergPublishResult:
    if not parquet_files:
        raise ValueError("at least one parquet file is required")

    file_io = _file_io(parquet_files[0])
    missing_files = [
        path for path in parquet_files if not file_io.new_input(path).exists()
    ]
    if missing_files:
        raise FileNotFoundError(missing_files[0])

    schema = pq.read_schema(parquet_files[0])
    catalog = load_in_memory(
        "static",
        {
            "warehouse": _join_uri(table_location, ".pyiceberg-catalog"),
            **pyiceberg_s3_properties(),
        },
    )
    catalog.create_namespace("default")
    table = catalog.create_table(
        f"default.{table_name}",
        schema,
        location=table_location,
        properties={
            "write.metadata.metrics.default": "none",
            "write.metadata.metrics.column.datetime": "full",
        },
    )
    _patch_pyiceberg_list_item_paths()
    table.add_files(parquet_files)
    table = table.refresh()

    latest_metadata_location = _join_uri(
        table_location, "metadata/latest.metadata.json"
    )
    with (
        _file_io(table.metadata_location)
        .new_input(table.metadata_location)
        .open() as stream
    ):
        metadata = stream.read()

    _write_bytes(latest_metadata_location, metadata)

    return IcebergPublishResult(
        metadata_location=table.metadata_location,
        latest_metadata_location=latest_metadata_location,
    )


def _collection_parquet_files(dest: str, version: str, collection_id: str) -> list[str]:
    prefix = f"{version}/{collection_id}/"
    return sorted(
        _join_uri(dest, result["path"])
        for batch in store_from_url(dest).list(prefix)
        for result in batch
        if result["path"].startswith(f"{prefix}year=")
        and result["path"].endswith(".parquet")
    )


def _patch_pyiceberg_list_item_paths() -> None:
    global _LIST_ITEM_PATH_PATCHED
    if _LIST_ITEM_PATH_PATCHED:
        return

    original = pyiceberg_pyarrow.parquet_path_to_id_mapping

    def patched(schema):
        mapping = original(schema)
        mapping.update(
            (path.replace(".list.element", ".list.item"), field_id)
            for path, field_id in list(mapping.items())
            if ".list.element" in path
        )
        return mapping

    pyiceberg_pyarrow.parquet_path_to_id_mapping = patched
    _LIST_ITEM_PATH_PATCHED = True


def _write_bytes(location: str, data: bytes) -> None:
    io = _file_io(location)
    with io.new_output(location).create(overwrite=True) as stream:
        stream.write(data)


def _file_io(location: str):
    return load_file_io(pyiceberg_s3_properties(), location)


def _join_uri(base: str, path: str) -> str:
    parsed = urlparse(base)
    if parsed.scheme == "file":
        joined = (Path(unquote(parsed.path)) / path).resolve()
        return f"file://{joined}"
    if parsed.scheme:
        return f"{base.rstrip('/')}/{path.lstrip('/')}"
    return f"file://{(Path(base) / path).resolve()}"
