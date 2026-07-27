from pathlib import Path

import duckdb
import pytest
from pyiceberg.table import StaticTable

from hls_stac_parquet.iceberg import publish_local_static_iceberg_table


def _write_parquet(path: Path, rows: list[tuple[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    values = ", ".join(f"('{item_id}')" for (item_id,) in rows)
    duckdb.sql(
        f"""
        COPY (
            SELECT id
            FROM (VALUES {values}) AS rows(id)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )


def _query_iceberg_ids(metadata_location: str | Path) -> list[str]:
    con = duckdb.connect()
    con.execute("LOAD iceberg")
    return [
        row[0]
        for row in con.sql(
            f"SELECT id FROM iceberg_scan('{metadata_location}') ORDER BY id"
        ).fetchall()
    ]


def _metadata_file_count(metadata_location: str) -> int:
    table = StaticTable.from_metadata(metadata_location)
    snapshot = table.current_snapshot()
    assert snapshot is not None
    return sum(
        len(manifest.fetch_manifest_entry(table.io))
        for manifest in snapshot.manifests(table.io)
    )


def test_static_iceberg_metadata_reads_original_hive_partitioned_files(tmp_path):
    parquet_root = tmp_path / "v2" / "HLSL30_2.0"
    file_a = parquet_root / "year=2025" / "month=1" / "a.parquet"
    file_b = parquet_root / "year=2025" / "month=1" / "b.parquet"
    _write_parquet(file_a, [("a",)])
    _write_parquet(file_b, [("b",)])

    result = publish_local_static_iceberg_table(
        [file_a, file_b], parquet_root / "iceberg"
    )

    assert _query_iceberg_ids(result.metadata_location) == ["a", "b"]
    assert _query_iceberg_ids(result.latest_metadata_path) == ["a", "b"]
    assert result.metadata_pointer_path.read_text() == result.metadata_location
    assert file_a.exists()
    assert file_b.exists()


@pytest.mark.parametrize("missing_file", ["missing.parquet"])
def test_missing_parquet_file_does_not_advance_stable_metadata(tmp_path, missing_file):
    table_location = tmp_path / "v2" / "HLSL30_2.0" / "iceberg"

    with pytest.raises(FileNotFoundError):
        publish_local_static_iceberg_table([tmp_path / missing_file], table_location)

    assert not (table_location / "metadata" / "latest.metadata.json").exists()
    assert not (table_location / "metadata" / "latest.metadata-location.txt").exists()


def test_republishing_overwritten_monthly_path_reads_new_rows_without_duplicates(
    tmp_path,
):
    parquet_file = (
        tmp_path
        / "v2"
        / "HLSL30_2.0"
        / "year=2025"
        / "month=1"
        / "HLSL30_2.0-2025-1.parquet"
    )
    table_location = tmp_path / "v2" / "HLSL30_2.0" / "iceberg"

    _write_parquet(parquet_file, [("old",)])
    publish_local_static_iceberg_table([parquet_file], table_location)

    _write_parquet(parquet_file, [("new",)])
    result = publish_local_static_iceberg_table([parquet_file], table_location)

    assert _query_iceberg_ids(result.latest_metadata_path) == ["new"]
    assert _metadata_file_count(result.metadata_location) == 1
