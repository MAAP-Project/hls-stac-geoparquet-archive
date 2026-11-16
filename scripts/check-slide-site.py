#!/usr/bin/env python3
"""Check that the built MkDocs slides link and Slidev assets resolve over HTTP."""

from __future__ import annotations

import argparse
import threading
from functools import partial
from html.parser import HTMLParser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urljoin, urlsplit
from urllib.request import urlopen


class SiteLinks(HTMLParser):
    """Collect links from the MkDocs landing page and Slidev entry page."""

    def __init__(self) -> None:
        """Initialize the collected link list and anchor state."""
        super().__init__()
        self.links: list[str] = []
        self._capture_anchor = False
        self._anchor_text = ""
        self._anchor_href: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Record links from each opening tag."""
        attributes = dict(attrs)
        if tag == "a":
            self._capture_anchor = True
            self._anchor_text = ""
            self._anchor_href = attributes.get("href")
            if self._anchor_href:
                self.links.append(self._anchor_href)
        for key in ("src", "href"):
            value = attributes.get(key)
            if value and "/assets/" in value:
                self.links.append(value)

    def handle_data(self, data: str) -> None:
        """Collect text used to identify the deck link."""
        if self._capture_anchor:
            self._anchor_text += data

    def handle_endtag(self, tag: str) -> None:
        """Mark the introduction-deck link after its text is available."""
        if tag == "a" and self._capture_anchor:
            self._capture_anchor = False
            if "introduction deck" in self._anchor_text and self._anchor_href:
                self.links.append("DECK_LINK:" + self._anchor_href)


def main() -> None:
    """Fetch the landing page, its deck link, and the deck's local assets."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site-dir", type=Path, default=Path("build"))
    parser.add_argument(
        "--prefix",
        default="",
        help="GitHub Pages project prefix, e.g. /hls-stac-geoparquet-archive",
    )
    args = parser.parse_args()
    prefix = args.prefix.rstrip("/")
    if not args.site_dir.is_dir():
        parser.error(f"site directory does not exist: {args.site_dir}")

    class SiteHandler(SimpleHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            """Keep smoke-check HTTP output limited to its final result."""
            return

        def translate_path(self, path: str) -> str:
            """Serve optional project-prefixed URLs from the site root."""
            request_path = urlsplit(path).path
            if prefix:
                if not request_path.startswith(prefix + "/"):
                    return str(args.site_dir / "__missing__")
                path = request_path[len(prefix) :]
            return super().translate_path(path)

    handler = partial(SiteHandler, directory=str(args.site_dir.resolve()))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    origin = f"http://127.0.0.1:{server.server_port}"
    landing_url = f"{origin}{prefix}/slides/"

    try:
        with urlopen(landing_url) as response:
            landing_html = response.read().decode()
            assert response.status == 200
        links = SiteLinks()
        links.feed(landing_html)
        deck_link = next(
            link.removeprefix("DECK_LINK:")
            for link in links.links
            if link.startswith("DECK_LINK:")
        )
        deck_url = urljoin(landing_url, deck_link)
        expected_deck_path = f"{prefix}/presentation/"
        if urlsplit(deck_url).path != expected_deck_path:
            raise AssertionError(
                f"deck link resolves to {deck_url}, expected path {expected_deck_path}"
            )

        with urlopen(deck_url) as response:
            deck_html = response.read().decode()
            assert response.status == 200
        deck_links = SiteLinks()
        deck_links.feed(deck_html)
        assets = [
            urljoin(deck_url, link)
            for link in deck_links.links
            if "/assets/" in link and not urlsplit(link).netloc
        ]
        if not assets:
            raise AssertionError("Slidev entry page does not reference any assets")
        for asset_url in assets:
            if not urlsplit(asset_url).path.startswith(
                f"{prefix}/presentation/assets/"
            ):
                raise AssertionError(
                    f"deck asset resolves outside presentation path: {asset_url}"
                )
            with urlopen(asset_url) as response:
                if response.status != 200:
                    raise AssertionError(f"asset request failed: {asset_url}")
                response.read()
        print(f"OK: {landing_url} -> {deck_url}; fetched {len(assets)} deck assets")
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
