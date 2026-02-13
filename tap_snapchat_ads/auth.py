"""SnapchatAds Authentication."""

from __future__ import annotations

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class SnapchatAdsAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for SnapchatAds."""

    def __init__(
        self,
        *args,
        refresh_token: str,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._refresh_token = refresh_token

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the SnapchatAds API."""
        return {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self._refresh_token,
        }
