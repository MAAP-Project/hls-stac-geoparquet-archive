import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime
from urllib.parse import urlparse

import hls_stac_parquet.fetch as fetch_module
import hls_stac_parquet.write as write_module
from hls_stac_parquet.cmr_api import HlsCollection
from hls_stac_parquet.constants import LINK_PATH_FORMAT


async def test_write_monthly_stac_geoparquet_preserves_hilbert_order_after_fetch_reordering(
    tmp_path, monkeypatch
):
    """The write pipeline should preserve Hilbert order even if fetch completes out of order."""

    link_a = "https://example.com/HLS.S30.T10AAA.2025001.v2.0_stac.json"
    link_b = "https://example.com/HLS.S30.T10AAB.2025001.v2.0_stac.json"
    link_c = "https://example.com/HLS.S30.T10AAC.2025001.v2.0_stac.json"

    hilbert_sorted_links = [link_a, link_b, link_c]
    stored_links = [link_c, link_a, link_b]

    source_file = tmp_path / LINK_PATH_FORMAT.format(
        collection_id=HlsCollection.HLSL30.collection_id,
        year=2025,
        month=10,
        day=2,
    )
    source_file.parent.mkdir(parents=True, exist_ok=True)
    source_file.write_text(json.dumps(stored_links))

    monkeypatch.setattr(
        write_module,
        "_hilbert_sort_key",
        lambda url: hilbert_sorted_links.index(url),
    )

    fetch_input_links: list[str] = []
    real_fetch_stac_items = fetch_module.fetch_stac_items

    async def recording_fetch_stac_items(
        stac_links,
        collection_id,
        max_concurrent=50,
        batch_size=1000,
    ):
        fetch_input_links.extend(link.geturl() for link in stac_links)

        async for batch in real_fetch_stac_items(
            stac_links,
            collection_id=collection_id,
            max_concurrent=max_concurrent,
            batch_size=batch_size,
        ):
            yield batch

    monkeypatch.setattr(write_module, "fetch_stac_items", recording_fetch_stac_items)
    monkeypatch.setattr(fetch_module, "from_url", lambda url, **kwargs: url)

    parsed_links = {
        "a": urlparse(link_a),
        "b": urlparse(link_b),
        "c": urlparse(link_c),
    }
    payloads = {
        parsed_links["a"].path: ({"id": "a"}, 0.02),
        parsed_links["b"].path: ({"id": "b"}, 0.03),
        parsed_links["c"].path: ({"id": "c"}, 0.01),
    }

    class FakeBytes:
        def __init__(self, payload: bytes):
            self._payload = payload

        def to_bytes(self) -> bytes:
            return self._payload

    class FakeResponse:
        def __init__(self, item: dict[str, str], delay: float):
            self._item = item
            self._delay = delay

        async def bytes_async(self):
            await asyncio.sleep(self._delay)
            return FakeBytes(json.dumps(self._item).encode("utf-8"))

    real_get_async = fetch_module.obs.get_async

    async def fake_get_async(store, path):
        if path in payloads:
            item, delay = payloads[path]
            return FakeResponse(item, delay)

        return await real_get_async(store, path)

    monkeypatch.setattr(fetch_module.obs, "get_async", fake_get_async)

    written_ids: list[str] = []

    class FakeWriter:
        async def write(self, batch_items):
            written_ids.extend(item["id"] for item in batch_items)

    @asynccontextmanager
    async def fake_geoparquet_writer(first_batch, out_path, store=None):
        written_ids.extend(item["id"] for item in first_batch)
        yield FakeWriter()

    monkeypatch.setattr(write_module, "geoparquet_writer", fake_geoparquet_writer)

    total_items_written = await write_module.write_monthly_stac_geoparquet(
        HlsCollection.HLSL30,
        yearmonth=datetime(2025, 10, 1),
        source=f"file://{tmp_path}",
        dest=f"file://{tmp_path}",
        version="v2",
        require_complete_links=False,
        batch_size=2,
    )

    assert fetch_input_links == hilbert_sorted_links
    assert total_items_written == 3
    assert written_ids == ["a", "b", "c"]
