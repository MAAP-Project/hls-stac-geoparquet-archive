"""Run the cache/write/publish pipeline against local MinIO."""

import argparse
import asyncio
import logging
import os
from datetime import datetime

import duckdb

from hls_stac_parquet.constants import HlsCollection
from hls_stac_parquet.iceberg import publish_static_iceberg_table
from hls_stac_parquet.links import cache_daily_stac_json_links
from hls_stac_parquet.write import write_monthly_stac_geoparquet

logger = logging.getLogger(__name__)


def _set_minio_env(endpoint: str) -> None:
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    os.environ.setdefault("AWS_ENDPOINT_URL", endpoint)


def _duckdb_iceberg_sql(metadata_location: str, endpoint: str) -> str:
    endpoint_host = endpoint.removeprefix("http://").removeprefix("https://")
    use_ssl = "true" if endpoint.startswith("https://") else "false"
    return f"""INSTALL httpfs;
LOAD httpfs;
INSTALL iceberg;
LOAD iceberg;
SET s3_region='us-east-1';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_endpoint='{endpoint_host}';
SET s3_url_style='path';
SET s3_use_ssl={use_ssl};
SELECT count(*) FROM iceberg_scan('{metadata_location}');"""


def _duckdb_count(metadata_location: str, endpoint: str) -> int:
    sql = _duckdb_iceberg_sql(metadata_location, endpoint)
    logger.info("DuckDB SQL for local Iceberg read:\n%s", sql)
    return duckdb.connect().sql(sql).fetchone()[0]


async def _run(args: argparse.Namespace) -> None:
    _set_minio_env(args.endpoint)
    collection = HlsCollection[args.collection]
    date = datetime.fromisoformat(args.date)
    dest = f"s3://{args.bucket}/{args.prefix}" if args.prefix else f"s3://{args.bucket}"

    logger.info("Caching STAC links to %s", dest)
    await cache_daily_stac_json_links(
        collection=collection,
        date=date,
        dest=dest,
        bounding_box=tuple(args.bbox),
        protocol="https",
        skip_existing=args.skip_existing,
    )

    logger.info("Writing monthly GeoParquet to %s", dest)
    total = await write_monthly_stac_geoparquet(
        collection=collection,
        yearmonth=date,
        source=dest,
        dest=dest,
        version=args.version,
        require_complete_links=False,
        skip_existing=args.skip_existing,
        batch_size=args.batch_size,
    )

    logger.info("Publishing Iceberg metadata")
    result = publish_static_iceberg_table(
        collection=collection,
        year=date.year,
        month=date.month,
        dest=dest,
        version=args.version,
    )

    count = _duckdb_count(result.latest_metadata_location, args.endpoint)
    logger.info("DuckDB read %s rows from %s", count, result.latest_metadata_location)
    print(result.latest_metadata_location)
    print(f"rows={count}; written={total}")


def main() -> None:
    """Parse arguments and run the local MinIO pipeline."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-existing", action="store_true")
    parser.set_defaults(
        endpoint="http://localhost:9000",
        bucket="hls-local",
        prefix="archive",
        version="v2",
        collection="HLSL30",
        date="2025-10-02",
        bbox=(-100, 40, -90, 50),
        batch_size=1000,
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
