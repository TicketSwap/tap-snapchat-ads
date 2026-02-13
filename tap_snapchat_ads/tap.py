"""SnapchatAds tap class."""

from __future__ import annotations

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_snapchat_ads.streams import (
    OrganizationsStream,
    AdAccountsStream,
    AdsStream,
    AdSquadsStream,
    AudienceSegmentsStream,
    BillingCentersStream,
    CampaignsStream,
    CreativesStream,
    FundingSourcesStream,
    MediaStream,
    MembersStream,
    PhoneNumbersStream,
    PixelsStream,
    PixelDomainStatsStream,
    ProductCatalogsStream,
    ProductSetsStream,
    RolesStream,
    AdAccountStatsDailyStream,
    CampaignStatsDailyStream,
    AdSquadStatsDailyStream,
    AdStatsDailyStream,
    AdAccountStatsHourlyStream,
    CampaignStatsHourlyStream,
    AdSquadStatsHourlyStream,
    AdStatsHourlyStream,
    AgeGroupsTargetingStream,
    GendersTargetingStream,
    LanguagesTargetingStream,
    AdvancedDemographicsTargetingStream,
    ConnectionTypesTargetingStream,
    OSTypesTargetingStream,
    IOSVersionsTargetingStream,
    AndroidVersionsTargetingStream,
    CarrierTargetingStream,
    DeviceMakeTargetingStream,
    CountriesTargetingGeoStream,
    InterestsDLXSTargetingStream,
    InterestsDLXCTargetingStream,
    InterestsDLXPTargetingStream,
    InterestsNLNTargetingStream,
    InterestsPLCTargetingStream,
    LocationCategoriesTargetingStream,
    RegionsTargetingGeoMultiCountryStream,
    MetrosTargetingGeoMultiCountryStream,
    PostalCodesTargetingGeoMultiCountryStream,
)
STREAM_TYPES = [
    OrganizationsStream,
    AdAccountsStream,
    AdsStream,
    AdSquadsStream,
    AudienceSegmentsStream,
    BillingCentersStream,
    CampaignsStream,
    # CreativesStream,
    FundingSourcesStream,
    # MediaStream,
    MembersStream,
    PhoneNumbersStream,
    PixelsStream,
    PixelDomainStatsStream,
    ProductCatalogsStream,
    ProductSetsStream,
    RolesStream,
    AdAccountStatsDailyStream,
    CampaignStatsDailyStream,
    AdSquadStatsDailyStream,
    AdStatsDailyStream,
    AdAccountStatsHourlyStream,
    CampaignStatsHourlyStream,
    AdSquadStatsHourlyStream,
    AdStatsHourlyStream,
    AgeGroupsTargetingStream,
    GendersTargetingStream,
    LanguagesTargetingStream,
    AdvancedDemographicsTargetingStream,
    ConnectionTypesTargetingStream,
    OSTypesTargetingStream,
    IOSVersionsTargetingStream,
    AndroidVersionsTargetingStream,
    CarrierTargetingStream,
    DeviceMakeTargetingStream,
    CountriesTargetingGeoStream,
    InterestsDLXSTargetingStream,
    InterestsDLXCTargetingStream,
    InterestsDLXPTargetingStream,
    InterestsNLNTargetingStream,
    InterestsPLCTargetingStream,
    LocationCategoriesTargetingStream,
    RegionsTargetingGeoMultiCountryStream,
    MetrosTargetingGeoMultiCountryStream,
    PostalCodesTargetingGeoMultiCountryStream,
]


class TapSnapchatAds(Tap):
    """SnapchatAds tap class."""
    name = "tap-snapchat-ads"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            secret=True,
            description="Client ID"
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            secret=True,
            description="Client Secret"
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=True,
            secret=True,
            description="Refresh token"
        ),
        th.Property(
            "swipe_up_attribution_window",
            th.StringType,
            required=False,
            default="28_DAY",
            description="Attribution window for swipe ups: 1_DAY, 7_DAY, 28_DAY (default)"
        ),
        th.Property(
            "view_attribution_window",
            th.StringType,
            required=False,
            default="1_DAY",
            description="Attribution window for views: 1_HOUR, 3_HOUR, 6_HOUR, 1_DAY (default), 7_DAY, 28_DAY"
        ),
        th.Property(
            "user_agent",
            th.StringType,
            required=False,
            description="User agent"
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=False,
            default="2022-01-01T00:00:00Z",
            description="Start date for stats"
        ),
        th.Property(
            "targeting_country_codes",
            th.ArrayType(th.StringType),
            required=False,
            default=[],
            description="List of lower - case 2 - letter ISO Country Codes for Ads Targeting."
        ),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapSnapchatAds.cli()
