#!/usr/bin/env python3
"""Check that the built MkDocs slides link and Slidev assets resolve over HTTP."""

from __future__ import annotations

import argparse
import logging
import re
import subprocess
import threading
from functools import partial
from html.parser import HTMLParser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urljoin, urlsplit
from urllib.request import urlopen


class SiteLinks(HTMLParser):
    """Collect navigation, deck, and asset links from the built pages."""

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
        """Mark relevant navigation and deck links after their text is available."""
        if tag == "a" and self._capture_anchor:
            self._capture_anchor = False
            anchor_text = " ".join(self._anchor_text.split()).lower()
            if self._anchor_href and "esdis tech spotlight" in anchor_text:
                self.links.append("DECK_LINK:" + self._anchor_href)


def main() -> None:
    """Verify Slides navigation, deck links, local assets, and optional rendering."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site-dir", type=Path, default=Path("build"))
    parser.add_argument(
        "--prefix",
        default="",
        help="GitHub Pages project prefix, e.g. /hls-stac-geoparquet-archive",
    )
    parser.add_argument(
        "--browser",
        help="Chrome/Chromium executable for checking that the first slide renders",
    )
    args = parser.parse_args()
    prefix = args.prefix.rstrip("/")
    if not args.site_dir.is_dir():
        parser.error(f"site directory does not exist: {args.site_dir}")

    deck_dir = args.site_dir / "presentations/esdis-tech-spotlight-2026-11-17"
    for authoring_path in ("slides/index.html", "README/index.html", "theme", "public"):
        if (deck_dir / authoring_path).exists():
            raise AssertionError(f"Authoring files leaked into site: {authoring_path}")

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

    try:
        homepage_url = f"{origin}{prefix}/"
        with urlopen(homepage_url) as response:
            homepage_html = response.read().decode()
            assert response.status == 200
        homepage_links = SiteLinks()
        homepage_links.feed(homepage_html)
        deck_link = next(
            link.removeprefix("DECK_LINK:")
            for link in homepage_links.links
            if link.startswith("DECK_LINK:")
        )
        deck_url = urljoin(homepage_url, deck_link)
        expected_deck_path = f"{prefix}/presentations/esdis-tech-spotlight-2026-11-17/"
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
            if not urlsplit(asset_url).path.startswith(f"{expected_deck_path}assets/"):
                raise AssertionError(
                    f"deck asset resolves outside presentation path: {asset_url}"
                )
            with urlopen(asset_url) as response:
                if response.status != 200:
                    raise AssertionError(f"asset request failed: {asset_url}")
                response.read()
        if args.browser:
            with (
                TemporaryDirectory(prefix="slidev-browser-") as profile,
                TemporaryDirectory(
                    dir=args.site_dir, prefix="navigation-check-"
                ) as probe,
            ):
                # Check computed visibility in a desktop-sized, same-origin homepage.
                probe_path = Path(probe) / "index.html"
                probe_path.write_text(
                    """<!doctype html>
<html><body>
<iframe src="../" width="1280" height="800" onload="
const doc = this.contentDocument;
const sidebar = doc.querySelector('.md-sidebar--primary');
const section = Array.from(sidebar.querySelectorAll('label')).find(label =>
    label.textContent.trim() === 'Slides');
const toggle = section &amp;&amp; doc.getElementById(section.htmlFor);
if (toggle &amp;&amp; !toggle.checked) section.click();
const visible = toggle &amp;&amp; toggle.checked &amp;&amp;
    Array.from(sidebar.querySelectorAll('a')).some(link =>
        link.textContent.trim().startsWith('ESDIS Tech Spotlight') &amp;&amp;
        link.checkVisibility({visibilityProperty: true}) &amp;&amp;
        link.getBoundingClientRect().width > 0);
document.body.dataset.navigationVisible = String(visible);
"></iframe>
</body></html>
""",
                    encoding="utf-8",
                )
                probe_url = f"{origin}{prefix}/{Path(probe).name}/"
                navigation = subprocess.run(
                    [
                        args.browser,
                        "--headless=new",
                        "--no-sandbox",
                        "--disable-gpu",
                        "--no-first-run",
                        "--window-size=1440,1000",
                        f"--user-data-dir={profile}",
                        "--virtual-time-budget=5000",
                        "--dump-dom",
                        probe_url,
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=45,
                )
                if 'data-navigation-visible="true"' not in navigation.stdout:
                    raise AssertionError(
                        f"Slides navigation is not visible at {homepage_url}"
                    )
                result = subprocess.run(
                    [
                        args.browser,
                        "--headless=new",
                        "--no-sandbox",
                        "--disable-gpu",
                        "--no-first-run",
                        f"--user-data-dir={profile}",
                        "--virtual-time-budget=5000",
                        "--dump-dom",
                        deck_url,
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=45,
                )
                if not re.search(r'class="[^"]*\bslidev-page-1\b', result.stdout):
                    raise AssertionError(
                        f"First slide did not render at {deck_url}; "
                        "check the Slidev router base path"
                    )
            logging.info(
                "Browser verified visible Slides navigation at %s", homepage_url
            )
            logging.info("Browser rendered the first slide at %s", deck_url)
        logging.info(
            "OK: %s -> %s; fetched %d deck assets",
            homepage_url,
            deck_url,
            len(assets),
        )
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
