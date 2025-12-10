import json
from datetime import datetime
from tempfile import TemporaryDirectory

import obstore
import pytest
from obstore.store import MemoryStore, from_url

from hls_stac_parquet.cmr_api import HlsCollection
from hls_stac_parquet.constants import LINK_PATH_FORMAT
from hls_stac_parquet.links import (
    _check_exists,
    cache_daily_stac_json_links,
    collect_stac_json_links,
    write_stac_links,
)
from hls_stac_parquet.write import write_monthly_stac_geoparquet

TEST_BOUNDING_BOX = (-93, 46, -92, 47)


@pytest.mark.vcr
@pytest.mark.parametrize("collection", list(HlsCollection))
async def test_collect_stac_json_links(collection: HlsCollection):
    links = await collect_stac_json_links(
        collection=collection,
        bounding_box=TEST_BOUNDING_BOX,
        temporal=("2025-10-01T00:00:00Z", "2025-10-10T00:00:00Z"),
    )

    for link in links:
        assert link.path.endswith("stac.json")


@pytest.mark.vcr
async def test_write_stac_json_links():
    links = await collect_stac_json_links(
        collection=HlsCollection.HLSL30,
        bounding_box=TEST_BOUNDING_BOX,
        temporal=("2025-10-01T00:00:00Z", "2025-10-10T00:00:00Z"),
    )

    store = MemoryStore()

    await write_stac_links(links, store, "test.json")

    res = obstore.get(store, "test.json").bytes()

    assert set(json.loads(bytes(res).decode())) == set(
        [link.geturl() for link in links]
    )


@pytest.mark.vcr
async def test_cache_daily_stac_json_links():
    with TemporaryDirectory() as tempdir:
        await cache_daily_stac_json_links(
            HlsCollection.HLSL30,
            date=datetime(2025, 10, 2),
            dest=f"file://{tempdir}",
            bounding_box=TEST_BOUNDING_BOX,
        )

        store = from_url(f"file://{tempdir}")
        exists = await _check_exists(
            store,
            LINK_PATH_FORMAT.format(
                collection_id=HlsCollection.HLSL30.collection_id,
                year=2025,
                month=10,
                day=2,
            ),
        )

        assert exists


@pytest.mark.vcr
async def test_write_monthly_stac_geoparquet():
    # tried using MemoryStore but it didn't work :/
    with TemporaryDirectory() as tempdir:
        with pytest.warns(RuntimeWarning, match="Successfully reconstructed a store"):
            await cache_daily_stac_json_links(
                HlsCollection.HLSL30,
                date=datetime(2025, 10, 2),
                dest=f"file://{tempdir}",
                bounding_box=TEST_BOUNDING_BOX,
            )

            await write_monthly_stac_geoparquet(
                HlsCollection.HLSL30,
                yearmonth=datetime(2025, 10, 1),
                source=f"file://{tempdir}",
                dest=f"file://{tempdir}",
                version="v2",
                require_complete_links=False,
            )

        with pytest.raises(ValueError, match="expected these links:"):
            await write_monthly_stac_geoparquet(
                HlsCollection.HLSL30,
                yearmonth=datetime(2025, 10, 1),
                source=f"file://{tempdir}",
                dest=f"file://{tempdir}",
                version="v2",
                require_complete_links=True,
            )
