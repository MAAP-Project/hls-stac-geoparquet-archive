from pathlib import Path

import duckdb
import pytest
from pyiceberg.table import StaticTable

import hls_stac_parquet.iceberg as iceberg
from hls_stac_parquet.constants import PARQUET_PATH_FORMAT, HlsCollection
from hls_stac_parquet.iceberg import publish_static_iceberg_table


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


def _file_uri(path: Path) -> str:
    return f"file://{path.resolve()}"


def _path_from_file_uri(location: str) -> Path:
    return Path(location.removeprefix("file://"))


def _query_iceberg_ids(metadata_location: str | Path) -> list[str]:
    con = duckdb.connect()
    con.execute("LOAD iceberg")
    return [
        row[0]
        for row in con.sql(
            f"SELECT id FROM iceberg_scan('{metadata_location}') ORDER BY id"
        ).fetchall()
    ]


def _monthly_parquet_path(
    root: Path, collection: HlsCollection, year: int, month: int
) -> Path:
    return root / PARQUET_PATH_FORMAT.format(
        version="v2", collection_id=collection.collection_id, year=year, month=month
    )


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

    result = iceberg._publish_static_iceberg_table(
        [_file_uri(file_a), _file_uri(file_b)],
        _file_uri(parquet_root / "iceberg"),
        "hls",
    )

    assert _query_iceberg_ids(result.metadata_location) == ["a", "b"]
    assert _query_iceberg_ids(_path_from_file_uri(result.latest_metadata_location)) == [
        "a",
        "b",
    ]
    assert file_a.exists()
    assert file_b.exists()


def test_static_iceberg_metadata_handles_list_columns(tmp_path):
    parquet_root = tmp_path / "v2" / "HLSL30_2.0"
    parquet_file = parquet_root / "year=2025" / "month=1" / "a.parquet"
    parquet_file.parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(
        f"""
        COPY (
            SELECT 'a' AS id, ['eo', 'proj'] AS stac_extensions
        ) TO '{parquet_file}' (FORMAT PARQUET)
        """
    )

    result = iceberg._publish_static_iceberg_table(
        [_file_uri(parquet_file)],
        _file_uri(parquet_root / "iceberg"),
        "hls",
    )

    assert _query_iceberg_ids(result.latest_metadata_location) == ["a"]


def test_missing_parquet_file_does_not_advance_stable_metadata(tmp_path):
    table_location = tmp_path / "v2" / "HLSL30_2.0" / "iceberg"

    with pytest.raises(FileNotFoundError):
        iceberg._publish_static_iceberg_table(
            [(tmp_path / "missing.parquet").as_uri()], table_location.as_uri(), "hls"
        )

    assert not (table_location / "metadata" / "latest.metadata.json").exists()


def test_publisher_writes_collection_metadata_without_touching_parquet(tmp_path):
    parquet_file = _monthly_parquet_path(tmp_path, HlsCollection.HLSL30, 2025, 1)
    _write_parquet(parquet_file, [("a",)])
    original_bytes = parquet_file.read_bytes()

    result = publish_static_iceberg_table(
        collection=HlsCollection.HLSL30,
        year=2025,
        month=1,
        dest=tmp_path.as_uri(),
        version="v2",
    )

    assert "/v2/HLSL30_2.0/iceberg/metadata/" in result.metadata_location
    assert _query_iceberg_ids(result.latest_metadata_location) == ["a"]
    assert parquet_file.read_bytes() == original_bytes


def test_publisher_keeps_collections_separate(tmp_path):
    hlsl_file = _monthly_parquet_path(tmp_path, HlsCollection.HLSL30, 2025, 1)
    hlss_file = _monthly_parquet_path(tmp_path, HlsCollection.HLSS30, 2025, 1)
    _write_parquet(hlsl_file, [("landsat",)])
    _write_parquet(hlss_file, [("sentinel",)])

    hlsl = publish_static_iceberg_table(
        collection=HlsCollection.HLSL30,
        year=2025,
        month=1,
        dest=tmp_path.as_uri(),
        version="v2",
    )
    hlss = publish_static_iceberg_table(
        collection=HlsCollection.HLSS30,
        year=2025,
        month=1,
        dest=tmp_path.as_uri(),
        version="v2",
    )

    assert "/HLSL30_2.0/" in hlsl.latest_metadata_location
    assert "/HLSS30_2.0/" in hlss.latest_metadata_location
    assert _query_iceberg_ids(hlsl.latest_metadata_location) == ["landsat"]
    assert _query_iceberg_ids(hlss.latest_metadata_location) == ["sentinel"]


def test_republishing_overwritten_monthly_path_reads_new_rows_without_duplicates(
    tmp_path,
):
    parquet_file = _monthly_parquet_path(tmp_path, HlsCollection.HLSL30, 2025, 1)

    _write_parquet(parquet_file, [("old",)])
    publish_static_iceberg_table(
        collection=HlsCollection.HLSL30,
        year=2025,
        month=1,
        dest=tmp_path.as_uri(),
        version="v2",
    )

    _write_parquet(parquet_file, [("new",)])
    result = publish_static_iceberg_table(
        collection=HlsCollection.HLSL30,
        year=2025,
        month=1,
        dest=tmp_path.as_uri(),
        version="v2",
    )

    assert _query_iceberg_ids(result.latest_metadata_location) == ["new"]
    assert _metadata_file_count(result.metadata_location) == 1


def test_failed_latest_metadata_write_does_not_advance_stable_entry(
    tmp_path, monkeypatch
):
    parquet_file = _monthly_parquet_path(tmp_path, HlsCollection.HLSL30, 2025, 1)
    _write_parquet(parquet_file, [("old",)])
    first = publish_static_iceberg_table(
        collection=HlsCollection.HLSL30,
        year=2025,
        month=1,
        dest=tmp_path.as_uri(),
        version="v2",
    )
    stable_metadata = _path_from_file_uri(first.latest_metadata_location).read_bytes()
    original_write_bytes = iceberg._write_bytes

    def fail_latest_metadata(location: str, data: bytes) -> None:
        if location.endswith("latest.metadata.json"):
            raise OSError("failed to write latest metadata")
        original_write_bytes(location, data)

    monkeypatch.setattr(iceberg, "_write_bytes", fail_latest_metadata)
    _write_parquet(parquet_file, [("new",)])

    with pytest.raises(OSError):
        publish_static_iceberg_table(
            collection=HlsCollection.HLSL30,
            year=2025,
            month=1,
            dest=tmp_path.as_uri(),
            version="v2",
        )

    assert (
        _path_from_file_uri(first.latest_metadata_location).read_bytes()
        == stable_metadata
    )


def test_publishing_newer_month_preserves_earlier_month(tmp_path):
    january = _monthly_parquet_path(tmp_path, HlsCollection.HLSL30, 2025, 1)
    february = _monthly_parquet_path(tmp_path, HlsCollection.HLSL30, 2025, 2)
    _write_parquet(january, [("jan",)])
    _write_parquet(february, [("feb",)])

    publish_static_iceberg_table(
        collection=HlsCollection.HLSL30,
        year=2025,
        month=1,
        dest=tmp_path.as_uri(),
        version="v2",
    )
    result = publish_static_iceberg_table(
        collection=HlsCollection.HLSL30,
        year=2025,
        month=2,
        dest=tmp_path.as_uri(),
        version="v2",
    )

    assert _query_iceberg_ids(result.latest_metadata_location) == ["feb", "jan"]
    assert _metadata_file_count(result.metadata_location) == 2
