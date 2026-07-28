"""Object-store construction helpers."""

import os
from urllib.parse import urlparse

from obstore.store import ObjectStore, from_url


def store_from_url(url: str) -> ObjectStore:
    """Create an object store, including local S3-compatible endpoint settings."""
    if urlparse(url).scheme != "s3":
        return from_url(url)

    endpoint = _s3_endpoint()
    kwargs = {}
    client_options = None
    if endpoint:
        kwargs["virtual_hosted_style_request"] = False
        if endpoint.startswith("http://"):
            client_options = {"allow_http": True}

    return from_url(url, client_options=client_options, **kwargs)


def pyiceberg_s3_properties() -> dict[str, str]:
    """Return PyIceberg S3 properties from standard AWS-style environment vars."""
    properties = {}
    if endpoint := _s3_endpoint():
        properties["s3.endpoint"] = endpoint
        properties["s3.force-virtual-addressing"] = "false"

    for env_name, property_name in {
        "AWS_ACCESS_KEY_ID": "s3.access-key-id",
        "AWS_SECRET_ACCESS_KEY": "s3.secret-access-key",
        "AWS_SESSION_TOKEN": "s3.session-token",
        "AWS_REGION": "s3.region",
        "AWS_DEFAULT_REGION": "s3.region",
    }.items():
        if value := os.environ.get(env_name):
            properties.setdefault(property_name, value)

    return properties


def _s3_endpoint() -> str | None:
    return (
        os.environ.get("AWS_ENDPOINT_URL_S3")
        or os.environ.get("AWS_ENDPOINT_URL")
        or os.environ.get("AWS_ENDPOINT")
    )
