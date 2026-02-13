"""REST client handling, including SnapchatAdsStream base class."""

from __future__ import annotations

from functools import cached_property
from typing import Any
from urllib.parse import urlparse, parse_qs

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from tap_snapchat_ads.auth import SnapchatAdsAuthenticator


class SnapchatAdsStream(RESTStream):
    """SnapchatAds stream class."""

    url_base = "https://adsapi.snapchat.com/v1"

    records_jsonpath = "$[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = "$.paging.next_link"  # Or override `get_next_page_token`.

    @cached_property
    def authenticator(self) -> SnapchatAdsAuthenticator:
        """Return a new authenticator object."""
        return SnapchatAdsAuthenticator(
            client_id=self.config["client_id"],
            client_secret=self.config["client_secret"],
            refresh_token=self.config["refresh_token"],
            auth_endpoint="https://accounts.snapchat.com/login/oauth2/access_token",
            oauth_scopes="snapchat-marketing-api",
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Any | None
    ) -> Any | None:
        """Return a token for identifying next page or None if no more pages."""
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_link_parsed = urlparse(first_match)
            next_page_token = parse_qs(next_page_link_parsed.query)
        else:
            next_page_token = response.headers.get("X-Next-Page", None)

        return next_page_token

    def get_url_params(
        self, context: dict | None, next_page_token: Any | None
    ) -> dict[str, Any]:
        params: dict = {}
        if next_page_token:
            params = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params
