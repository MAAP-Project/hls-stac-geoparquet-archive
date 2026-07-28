from hls_stac_parquet.storage import pyiceberg_s3_properties


def test_pyiceberg_s3_properties_map_minio_environment(monkeypatch):
    """Local MinIO env vars are translated to PyIceberg's S3 property names."""
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    assert pyiceberg_s3_properties() == {
        "s3.endpoint": "http://localhost:9000",
        "s3.force-virtual-addressing": "false",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.region": "us-east-1",
    }
