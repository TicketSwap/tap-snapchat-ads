"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import SuiteConfig, get_tap_test_class

from tap_snapchat_ads.tap import TapSnapchatAds

SAMPLE_CONFIG = {
    "start_date": (
        datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=7)
    ).strftime("%Y-%m-%dT00:00:00Z"),
    "targeting_country_codes": ["nl"],
}

TestTapSnapchatAds = get_tap_test_class(
    tap_class=TapSnapchatAds,
    config=SAMPLE_CONFIG,
    suite_config=SuiteConfig(ignore_no_records=True),
)
