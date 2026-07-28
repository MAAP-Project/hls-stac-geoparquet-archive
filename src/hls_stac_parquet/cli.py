"""CLI commands for HLS STAC to parquet workflow."""

import asyncio
from functools import wraps
from typing import Annotated

import typer

from hls_stac_parquet.constants import HlsCollection
from hls_stac_parquet.iceberg import publish_static_iceberg_table
from hls_stac_parquet.links import cache_daily_stac_json_links
from hls_stac_parquet.write import write_monthly_stac_geoparquet

app = typer.Typer()


def async_command(func):
    """Decorator to convert async function to sync typer command."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))

    return app.command()(wrapper)


@app.command("publish-static-iceberg-table")
def publish_static_iceberg_table_cmd(
    collection: Annotated[
        HlsCollection, typer.Argument(help="HLS collection to publish")
    ],
    year: Annotated[int, typer.Argument(help="Year to publish")],
    month: Annotated[int, typer.Argument(help="Month to publish")],
    dest: Annotated[str, typer.Argument(help="Archive destination URL")],
    version: Annotated[str, typer.Argument(help="Version string for output path")],
) -> None:
    """Publish static Iceberg metadata for one collection/month."""
    result = publish_static_iceberg_table(
        collection=collection, year=year, month=month, dest=dest, version=version
    )
    typer.echo(result.latest_metadata_location)


# Register commands - signatures are automatically preserved
cache_daily_stac_json_links_cmd = async_command(cache_daily_stac_json_links)
write_monthly_stac_geoparquet_cmd = async_command(write_monthly_stac_geoparquet)


if __name__ == "__main__":
    app()
