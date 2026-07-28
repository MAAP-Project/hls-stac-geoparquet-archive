import pytest

import hls_stac_parquet.write_handler as write_handler
from hls_stac_parquet.constants import HlsCollection
from hls_stac_parquet.iceberg import IcebergPublishResult


def _set_handler_env(monkeypatch, tmp_path):
    monkeypatch.setenv("SOURCE", tmp_path.as_uri())
    monkeypatch.setenv("DEST", tmp_path.as_uri())
    monkeypatch.setenv("VERSION", "v2")


def test_handler_publishes_iceberg_after_monthly_write(tmp_path, monkeypatch):
    """Successful write handler calls the Iceberg publisher and returns its status."""
    _set_handler_env(monkeypatch, tmp_path)
    calls = []

    async def fake_write_monthly_stac_geoparquet(**kwargs):
        calls.append(("write", kwargs))
        return 7

    def fake_publish_static_iceberg_table(**kwargs):
        calls.append(("publish", kwargs))
        return IcebergPublishResult("metadata.json", "latest.json")

    monkeypatch.setattr(
        write_handler,
        "write_monthly_stac_geoparquet",
        fake_write_monthly_stac_geoparquet,
    )
    monkeypatch.setattr(
        write_handler,
        "publish_static_iceberg_table",
        fake_publish_static_iceberg_table,
    )

    response = write_handler.handler(
        {"collection": "HLSL30", "yearmonth": "2025-01-01"}
    )

    assert [name for name, _ in calls] == ["write", "publish"]
    assert calls[1][1] == {
        "collection": HlsCollection.HLSL30,
        "year": 2025,
        "month": 1,
        "dest": tmp_path.as_uri(),
        "version": "v2",
    }
    assert response["total_items_written"] == 7
    assert response["iceberg"]["latest_metadata_location"] == "latest.json"


def test_handler_raises_when_iceberg_publication_fails(tmp_path, monkeypatch):
    """Metadata failures propagate so Step Functions can retry or fail."""
    _set_handler_env(monkeypatch, tmp_path)

    async def fake_write_monthly_stac_geoparquet(**kwargs):
        return 7

    def fake_publish_static_iceberg_table(**kwargs):
        raise RuntimeError("metadata failed")

    monkeypatch.setattr(
        write_handler,
        "write_monthly_stac_geoparquet",
        fake_write_monthly_stac_geoparquet,
    )
    monkeypatch.setattr(
        write_handler,
        "publish_static_iceberg_table",
        fake_publish_static_iceberg_table,
    )

    with pytest.raises(RuntimeError, match="metadata failed"):
        write_handler.handler({"collection": "HLSL30", "yearmonth": "2025-01-01"})
