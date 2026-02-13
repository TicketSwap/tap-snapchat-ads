"""Stream type classes for tap-snapchat-ads."""

from __future__ import annotations

import datetime
from collections.abc import Iterable
from typing import Any
from urllib.parse import urlparse, parse_qs

import pytz
import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_snapchat_ads.client import SnapchatAdsStream


class OrganizationsStream(SnapchatAdsStream):
    name = "organizations"
    path = "/me/organizations"
    records_jsonpath = "$.organizations[*].organization"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("name", th.StringType),
        th.Property("country", th.StringType),
        th.Property("postal_code", th.StringType),
        th.Property("locality", th.StringType),
        th.Property("contact_name", th.StringType),
        th.Property("contact_email", th.StringType),
        th.Property("contact_phone", th.StringType),
        th.Property("address_line_1", th.StringType),
        th.Property("administrative_district_level_1", th.StringType),
        th.Property("accepted_term_version", th.StringType),
        th.Property("contact_phone_optin", th.BooleanType),
        th.Property("configuration_settings", th.ObjectType(
            th.Property("notifications_enabled", th.BooleanType)
        )),
        th.Property("type", th.StringType),
        th.Property("state", th.StringType),
        th.Property("roles", th.ArrayType(th.StringType)),
        th.Property("my_display_name", th.StringType),
        th.Property("my_invited_email", th.StringType),
        th.Property("my_member_id", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {
            'organization_id': record["id"]
        }


class AdAccountsStream(SnapchatAdsStream):
    name = "ad_accounts"
    path = "/organizations/{organization_id}/adaccounts"
    parent_stream_type = OrganizationsStream
    ignore_parent_replication_key = True
    records_jsonpath = "$.adaccounts[*].adaccount"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("name", th.StringType),
        th.Property("type", th.StringType),
        th.Property("status", th.StringType),
        th.Property("organization_id", th.StringType),
        th.Property("funding_source_ids", th.ArrayType(th.StringType)),
        th.Property("currency", th.StringType),
        th.Property("lifetime_spend_cap_micro", th.IntegerType),
        th.Property("timezone", th.StringType),
        th.Property("advertiser", th.StringType),
        th.Property("advertiser_organization_id", th.StringType),
        th.Property("agency_representing_client", th.BooleanType),
        th.Property("client_based_in_country", th.StringType),
        th.Property("client_paying_invoices", th.BooleanType),
        th.Property("agency_client_metadata", th.ObjectType(
            th.Property("name", th.StringType),
            th.Property("email", th.StringType),
            th.Property("address_line_1", th.StringType),
            th.Property("city", th.StringType),
            th.Property("administrative_district_level_1", th.StringType),
            th.Property("country", th.StringType),
            th.Property("zipcode", th.StringType),
            th.Property("tax_id", th.StringType),
        )),
        th.Property("paying_advertiser_name", th.StringType),
        th.Property("billing_type", th.StringType),
        th.Property("regulations", th.ObjectType(
            th.Property("restricted_delivery_signals", th.BooleanType)
        )),
    ).to_dict()

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {
            'ad_account_id': record["id"]
        }


class AdsStream(SnapchatAdsStream):
    name = "ads"
    path = "/adaccounts/{ad_account_id}/ads"
    parent_stream_type = AdAccountsStream
    ignore_parent_replication_key = True
    records_jsonpath = "$.ads[*].ad"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("name", th.StringType),
        th.Property("paying_advertiser_name", th.StringType),
        th.Property("ad_account_id", th.StringType),
        th.Property("ad_squad_id", th.StringType),
        th.Property("creative_id", th.StringType),
        th.Property("status", th.StringType),
        th.Property("type", th.StringType),
        th.Property("render_type", th.StringType),
        th.Property("review_status", th.StringType),
        th.Property("review_status_reasons", th.ArrayType(th.StringType)),
        th.Property("third_party_paid_impression_tracking_urls", th.ArrayType(
            th.ObjectType(
                th.Property("tracking_url_metadata", th.ObjectType()),
                th.Property("expanded_tracking_url", th.StringType),
                th.Property("tracking_url", th.StringType)
            )
        )),
        th.Property("third_party_swipe_tracking_urls", th.ArrayType(
            th.ObjectType(
                th.Property("tracking_url_metadata", th.ObjectType()),
                th.Property("expanded_tracking_url", th.StringType),
                th.Property("tracking_url", th.StringType)
            )
        )),
    ).to_dict()

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {
            "ad_id": record["id"]
        }


class AdSquadsStream(SnapchatAdsStream):
    name = "ad_squads"
    path = "/adaccounts/{ad_account_id}/adsquads"
    parent_stream_type = AdAccountsStream
    ignore_parent_replication_key = True
    records_jsonpath = "$.adsquads[*].adsquad"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("name", th.StringType),
        th.Property("status", th.StringType),
        th.Property("campaign_id", th.StringType),
        th.Property("ad_account_id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("targeting", th.ObjectType(
            th.Property("regulated_content", th.BooleanType),
            th.Property("enable_targeting_expansion", th.BooleanType),
            th.Property("demographics", th.ArrayType(th.ObjectType())),
            th.Property("devices", th.ArrayType(th.ObjectType())),
            th.Property("geos", th.ArrayType(th.ObjectType())),
            th.Property("interests", th.ArrayType(th.ObjectType())),
            th.Property("segments", th.ArrayType(th.ObjectType())),
        )),
        th.Property("targeting_reach_status", th.StringType),
        th.Property("placement", th.StringType),
        th.Property("placement_v2", th.ObjectType(
            th.Property("config", th.StringType),
            th.Property("platforms", th.StringType),
            th.Property("inclusion", th.ObjectType(
                th.Property("content_types", th.ArrayType(th.StringType))
            )),
            th.Property("exclusion", th.ObjectType(
                th.Property("content_types", th.ArrayType(th.StringType))
            )),
        )),
        th.Property("billing_event", th.StringType),
        th.Property("bid_micro", th.IntegerType),
        th.Property("auto_bid", th.BooleanType),
        th.Property("target_bid", th.BooleanType),
        th.Property("bid_strategy", th.StringType),
        th.Property("daily_budget_micro", th.IntegerType),
        th.Property("lifetime_budget_micro", th.IntegerType),
        th.Property("start_time", th.DateTimeType),
        th.Property("end_time", th.DateTimeType),
        th.Property("optimization_goal", th.StringType),
        th.Property("impression_goal", th.StringType),
        th.Property("reach_goal", th.StringType),
        th.Property("reach_and_frequency_status", th.StringType),
        th.Property("reach_and_frequency_micro", th.IntegerType),
        th.Property("delivery_constraint", th.StringType),
        th.Property("pacing_type", th.StringType),
        th.Property("pixel_id", th.StringType),
        th.Property("cap_and_exclusion_config", th.ObjectType()),
        th.Property("product_properties", th.ObjectType()),
    ).to_dict()

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {
            "ad_squad_id": record["id"]
        }


class AudienceSegmentsStream(SnapchatAdsStream):
    name = "audience_segments"
    path = "/adaccounts/{ad_account_id}/segments"
    parent_stream_type = AdAccountsStream
    ignore_parent_replication_key = True
    records_jsonpath = "$.segments[*].segment"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("name", th.StringType),
        th.Property("ad_account_id", th.StringType),
        th.Property("organization_id", th.StringType),
        th.Property("description", th.StringType),
        th.Property("status", th.StringType),
        th.Property("targetable_status", th.StringType),
        th.Property("upload_status", th.StringType),
        th.Property("source_type", th.StringType),
        th.Property("retention_in_days", th.IntegerType),
        th.Property("approximate_number_users", th.IntegerType),
        th.Property("visible_to", th.ArrayType(th.StringType)),
    ).to_dict()


class BillingCentersStream(SnapchatAdsStream):
    name = "billing_centers"
    path = "/organizations/{organization_id}/billingcenters"
    parent_stream_type = OrganizationsStream
    ignore_parent_replication_key = True
    records_jsonpath = "$.billingcenter[*].billingcenter"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("name", th.StringType),
        th.Property("organization_id", th.StringType),
        th.Property("email_address", th.StringType),
        th.Property("address_line_1", th.StringType),
        th.Property("locality", th.StringType),
        th.Property("administrative_district_level_1", th.StringType),
        th.Property("country", th.IntegerType),
        th.Property("postal_code", th.StringType),
        th.Property("alternative_email_addresses", th.StringType),
    ).to_dict()


class CampaignsStream(SnapchatAdsStream):
    name = "campaigns"
    path = "/adaccounts/{ad_account_id}/campaigns"
    parent_stream_type = AdAccountsStream
    ignore_parent_replication_key = True
    records_jsonpath = "$.campaigns[*].campaign"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("name", th.StringType),
        th.Property("ad_account_id", th.StringType),
        th.Property("daily_budget_micro", th.IntegerType),
        th.Property("status", th.StringType),
        th.Property("objective", th.StringType),
        th.Property("start_time", th.DateTimeType),
        th.Property("end_time", th.DateTimeType),
        th.Property("lifetime_spend_cap_micro", th.IntegerType),
        th.Property("buy_model", th.StringType),
        th.Property("measurement_spec", th.ObjectType()),
        th.Property("regulations", th.ObjectType()),
    ).to_dict()

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {
            "campaign_id": record["id"]
        }


class CreativesStream(SnapchatAdsStream):
    name = "creatives"
    path = "/adaccounts/{ad_account_id}/creatives"
    parent_stream_type = AdAccountsStream
    ignore_parent_replication_key = True
    records_jsonpath = "$.creatives[*].creative"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("name", th.StringType),
        th.Property("ad_account_id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("packaging_status", th.StringType),
        th.Property("review_status", th.StringType),
        th.Property("shareable", th.BooleanType),
        th.Property("headline", th.StringType),
        th.Property("brand_name", th.StringType),
        th.Property("call_to_action", th.StringType),
        th.Property("render_type", th.StringType),
        th.Property("top_snap_media_id", th.StringType),
        th.Property("top_snap_crop_position", th.StringType),
        th.Property("forced_view_eligibility", th.StringType),
        th.Property("preview_creative_id", th.StringType),
        th.Property("playback_type", th.StringType),
        th.Property("ad_product", th.StringType),
        th.Property("ad_to_lens_properties", th.ObjectType()),
        th.Property("ad_to_message_properties", th.ObjectType()),
        th.Property("app_install_properties", th.ObjectType()),
        th.Property("collection_properties", th.ObjectType()),
        th.Property("composite_properties", th.ObjectType()),
        th.Property("deep_link_properties", th.ObjectType()),
        th.Property("dynamic_render_properties", th.ObjectType()),
        th.Property("longform_video_properties", th.ObjectType()),
        th.Property("preview_properties", th.ObjectType()),
        th.Property("web_view_properties", th.ObjectType()),
    ).to_dict()


class FundingSourcesStream(SnapchatAdsStream):
    name = "funding_sources"
    path = "/organizations/{organization_id}/fundingsources"
    parent_stream_type = OrganizationsStream
    ignore_parent_replication_key = True
    records_jsonpath = "$.fundingsources[*].fundingsource"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("name", th.StringType),
        th.Property("organization_id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("status", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("total_budget_micro", th.IntegerType),
        th.Property("budget_spent_micro", th.IntegerType),
        th.Property("available_credit_micro", th.IntegerType),
        th.Property("card_type", th.StringType),
        th.Property("last_4", th.StringType),
        th.Property("expiration_year", th.StringType),
        th.Property("expiration_month", th.StringType),
        th.Property("daily_spend_limit_micro", th.IntegerType),
        th.Property("daily_spend_limit_currency", th.StringType),
        th.Property("value_micro", th.IntegerType),
        th.Property("start_date", th.DateTimeType),
        th.Property("end_date", th.DateTimeType),
        th.Property("email", th.StringType),
    ).to_dict()


class MediaStream(SnapchatAdsStream):
    name = "media"
    path = "/adaccounts/{ad_account_id}/media"
    parent_stream_type = AdAccountsStream
    ignore_parent_replication_key = True
    records_jsonpath = "$.media[*].media"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("name", th.StringType),
        th.Property("ad_account_id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("media_status", th.StringType),
        th.Property("file_name", th.StringType),
        th.Property("download_link", th.StringType),
        th.Property("image_metadata", th.ObjectType(
            th.Property("height_px", th.IntegerType),
            th.Property("width_px", th.IntegerType),
            th.Property("image_format", th.StringType),
        )),
        th.Property("video_metadata", th.ObjectType()),
        th.Property("lens_package_metadata", th.ObjectType()),
        th.Property("file_size_in_bytes", th.IntegerType),
        th.Property("is_demo_media", th.BooleanType),
        th.Property("hash", th.StringType),
        th.Property("visibility", th.StringType),
    ).to_dict()


class MembersStream(SnapchatAdsStream):
    name = "members"
    path = "/organizations/{organization_id}/members"
    parent_stream_type = OrganizationsStream
    ignore_parent_replication_key = True
    records_jsonpath = "$.members[*].member"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("email", th.StringType),
        th.Property("organization_id", th.StringType),
        th.Property("display_name", th.StringType),
        th.Property("member_status", th.StringType),
    ).to_dict()


class PhoneNumbersStream(SnapchatAdsStream):
    name = "phone_numbers"
    path = "/adaccounts/{ad_account_id}/phone_numbers"
    parent_stream_type = AdAccountsStream
    ignore_parent_replication_key = True
    records_jsonpath = "$.phone_numbers[*].phone_number"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("ad_account_id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("country_code", th.StringType),
        th.Property("numerical_country_code", th.StringType),
        th.Property("phone_number", th.StringType),
        th.Property("verification_status", th.StringType),
    ).to_dict()


class PixelsStream(SnapchatAdsStream):
    name = "pixel_domain_stats"
    path = "/adaccounts/{ad_account_id}/pixels"
    parent_stream_type = AdAccountsStream
    ignore_parent_replication_key = True
    records_jsonpath = "$.pixels[*].pixel"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("ad_account_id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("status", th.StringType),
        th.Property("effective_status", th.StringType),
        th.Property("pixel_javascript", th.StringType),
        th.Property("visible_to", th.ArrayType(th.StringType)),
    ).to_dict()

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {
            "pixel_id": record["id"]
        }


class PixelDomainStatsStream(SnapchatAdsStream):
    name = "pixel_domain_stats"
    path = "/pixels/{pixel_id}/domains/stats"
    parent_stream_type = PixelsStream
    ignore_parent_replication_key = True
    records_jsonpath = "$.timeseries_stats[*].timeseries_stat"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("start_time", th.DateTimeType),
        th.Property("end_time", th.DateTimeType),
        th.Property("type", th.StringType),
        th.Property("pixel_id", th.StringType),
        th.Property("domains", th.ArrayType(
            th.ObjectType(
              th.Property("domain_name", th.StringType),
              th.Property("total_events", th.IntegerType),
            ),
        )),
    ).to_dict()


class ProductCatalogsStream(SnapchatAdsStream):
    name = "product_catalogs"
    path = "/organizations/{organization_id}/catalogs"
    parent_stream_type = OrganizationsStream
    ignore_parent_replication_key = True
    records_jsonpath = "$.catalogs[*].catalog"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("organization_id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("source", th.StringType),
        th.Property("default_product_set_id", th.StringType),
        th.Property("event_sources", th.ArrayType(
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("type", th.StringType),
            )
        )),
    ).to_dict()

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {
            "product_catalog_id": record["id"]
        }


class ProductSetsStream(SnapchatAdsStream):
    name = "product_sets"
    path = "/catalogs/{product_catalog_id}/product_sets"
    parent_stream_type = ProductCatalogsStream
    ignore_parent_replication_key = True
    records_jsonpath = "$.product_sets[*].product_set"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("catalog_id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("filter", th.ObjectType()),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
    ).to_dict()


class RolesStream(SnapchatAdsStream):
    name = "roles"
    path = "/organizations/{organization_id}/roles"
    parent_stream_type = OrganizationsStream
    ignore_parent_replication_key = True
    records_jsonpath = "$.roles[*].role"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("container_kind", th.StringType),
        th.Property("container_id", th.StringType),
        th.Property("member_id", th.StringType),
        th.Property("organization_id", th.StringType),
        th.Property("type", th.StringType),
    ).to_dict()


ALL_STATS_FIELDS = 'android_installs,attachment_avg_view_time_millis,attachment_impressions,attachment_quartile_1,attachment_quartile_2,attachment_quartile_3,attachment_total_view_time_millis,attachment_view_completion,avg_screen_time_millis,avg_view_time_millis,impressions,ios_installs,quartile_1,quartile_2,quartile_3,screen_time_millis,spend,swipe_up_percent,swipes,total_installs,video_views,video_views_time_based,video_views_15s,view_completion,view_time_millis,conversion_purchases,conversion_purchases_value,conversion_save,conversion_start_checkout,conversion_add_cart,conversion_view_content,conversion_add_billing,conversion_sign_ups,conversion_searches,conversion_level_completes,conversion_app_opens,conversion_page_views,conversion_subscribe,conversion_ad_click,conversion_ad_view,conversion_complete_tutorial,conversion_invite,conversion_login,conversion_share,conversion_reserve,conversion_achievement_unlocked,conversion_add_to_wishlist,conversion_spend_credits,conversion_rate,conversion_start_trial,conversion_list_view,custom_event_1,custom_event_2,custom_event_3,custom_event_4,custom_event_5,attachment_frequency,attachment_uniques,frequency,uniques'


class StatsStream(SnapchatAdsStream):
    ignore_parent_replication_key = True
    primary_keys = ['id', 'start_time']
    replication_key = 'start_time'
    granularity = 'DAY'
    date_step_days = 30
    fields = ALL_STATS_FIELDS
    properties = [
        th.Property("id", th.StringType),
        th.Property("start_time", th.DateTimeType),
        th.Property("end_time", th.DateTimeType),
        th.Property("type", th.StringType),
        th.Property("granularity", th.StringType),
        th.Property("swipe_up_attribution_window", th.StringType),
        th.Property("view_attribution_window", th.StringType),
        th.Property("finalized_data_end_time", th.DateTimeType)
    ]
    properties += [th.Property(metric, th.NumberType) for metric in fields.split(',')]
    schema = th.PropertiesList(*properties).to_dict()
    max_timestamp = datetime.datetime.now()

    def get_url_params(
            self, context: dict | None, next_page_token: Any | None
    ) -> dict[str, Any]:
        if next_page_token:
            start_time = next_page_token['start_time']
        else:
            start_time = self.get_starting_timestamp(context)
        end_time = min(
            (start_time + datetime.timedelta(days=self.date_step_days)).replace(tzinfo=pytz.UTC),
            self.max_timestamp.replace(tzinfo=pytz.UTC)
        )
        params = {
            'fields': self.fields,
            'granularity': self.granularity,
            'omit_empty': 'false',
            "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%dT%H:%M:%S"),
            'conversion_source_types': 'web,app,total',
            'swipe_up_attribution_window': self.config['swipe_up_attribution_window'],
            'view_attribution_window': self.config['view_attribution_window'],
        }
        if next_page_token:
            if next_page_token.get('cursor'):
                params['cursor'] = next_page_token['cursor']
            if next_page_token.get('limit'):
                params['limit'] = next_page_token['limit']
        return params

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
            first_match = None

        if not first_match and not next_page_token:
            end_time = datetime.datetime.strptime(parse_qs(urlparse(response.request.url).query)['end_time'][0], "%Y-%m-%dT%H:%M:%S")
            if end_time < self.max_timestamp:
                next_page_token = {"start_time": end_time}
        return next_page_token

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        response_json = response.json()
        for timeseries_stat in response_json['timeseries_stats']:
            for data_point in timeseries_stat['timeseries_stat']['timeseries']:
                new_row = timeseries_stat['timeseries_stat'].copy()
                new_row.pop('timeseries')
                new_row['start_time'] = data_point['start_time']
                new_row['end_time'] = data_point['end_time']
                new_row = dict(new_row, **data_point['stats'])
                yield new_row


class StatsDailyStream(StatsStream):
    ignore_parent_replication_key = True
    primary_keys = ['id', 'start_time']
    replication_key = 'start_time'
    granularity = 'DAY'
    date_step_days = 30
    fields = ALL_STATS_FIELDS
    max_timestamp = datetime.datetime.now(tz=None).replace(hour=0, minute=0, second=0, microsecond=0)


class AdAccountStatsDailyStream(StatsDailyStream):
    name = "ad_account_stats_daily"
    path = "/adaccounts/{ad_account_id}/stats"
    parent_stream_type = AdAccountsStream
    fields = 'spend'


class CampaignStatsDailyStream(StatsDailyStream):
    name = "campaign_stats_daily"
    path = "/campaigns/{campaign_id}/stats"
    parent_stream_type = CampaignsStream


class AdSquadStatsDailyStream(StatsDailyStream):
    name = "ad_squad_stats_daily"
    path = "/adsquads/{ad_squad_id}/stats"
    parent_stream_type = AdSquadsStream


class AdStatsDailyStream(StatsDailyStream):
    name = "ad_stats_daily"
    path = "/ads/{ad_id}/stats"
    parent_stream_type = AdsStream


class StatsHourlyStream(StatsStream):
    ignore_parent_replication_key = True
    primary_keys = ['id', 'start_time']
    replication_key = 'start_time'
    granularity = 'HOUR'
    date_step_days = 7
    fields = ALL_STATS_FIELDS
    max_timestamp = datetime.datetime.now(tz=None).replace(minute=0, second=0, microsecond=0)


class AdAccountStatsHourlyStream(StatsHourlyStream):
    name = "ad_account_stats_hourly"
    path = "/adaccounts/{ad_account_id}/stats"
    parent_stream_type = AdAccountsStream
    fields = 'spend'


class CampaignStatsHourlyStream(StatsHourlyStream):
    name = "campaign_stats_hourly"
    path = "/campaigns/{campaign_id}/stats"
    parent_stream_type = CampaignsStream


class AdSquadStatsHourlyStream(StatsHourlyStream):
    name = "ad_squad_stats_hourly"
    path = "/adsquads/{ad_squad_id}/stats"
    parent_stream_type = AdSquadsStream


class AdStatsHourlyStream(StatsHourlyStream):
    name = "ad_stats_hourly"
    path = "/ads/{ad_id}/stats"
    parent_stream_type = AdsStream


class TargetingStream(SnapchatAdsStream):
    ignore_parent_replication_key = True
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("targeting_group", th.StringType),
        th.Property("targeting_type", th.StringType),
        th.Property("name", th.StringType),
        th.Property("path", th.StringType),
        th.Property("source", th.StringType),
        th.Property("parent_id", th.StringType),
        th.Property("country_code", th.StringType),
    ).to_dict()


class AgeGroupsTargetingStream(TargetingStream):
    name = 'targeting_age_groups'
    path = "/targeting/demographics/age_group"
    records_jsonpath = "$.targeting_dimensions[*].age_group"


class GendersTargetingStream(TargetingStream):
    name = 'targeting_genders'
    path = "/targeting/demographics/gender"
    records_jsonpath = "$.targeting_dimensions[*].gender"


class LanguagesTargetingStream(TargetingStream):
    name = 'targeting_languages'
    path = "/targeting/demographics/languages"
    records_jsonpath = "$.targeting_dimensions[*].languages"


class AdvancedDemographicsTargetingStream(TargetingStream):
    name = 'targeting_advanced_demographics'
    path = "/targeting/demographics/advanced_demographics"
    records_jsonpath = "$.targeting_dimensions[*].advanced_demographics"


class ConnectionTypesTargetingStream(TargetingStream):
    name = 'targeting_connection_types'
    path = "/targeting/device/connection_type"
    records_jsonpath = "$.targeting_dimensions[*].connection_type"


class OSTypesTargetingStream(TargetingStream):
    name = 'targeting_os_types'
    path = "/targeting/device/os_type"
    records_jsonpath = "$.targeting_dimensions[*].os_type"


class IOSVersionsTargetingStream(TargetingStream):
    name = 'targeting_ios_versions'
    path = "/targeting/device/iOS/os_version"
    records_jsonpath = "$.targeting_dimensions[*].os_version"


class AndroidVersionsTargetingStream(TargetingStream):
    name = 'targeting_android_versions'
    path = "/targeting/device/ANDROID/os_version"
    records_jsonpath = "$.targeting_dimensions[*].os_version"


class CarrierTargetingStream(TargetingStream):
    name = 'targeting_carriers'
    path = "/targeting/device/carrier"
    records_jsonpath = "$.targeting_dimensions[*].carrier"


class DeviceMakeTargetingStream(TargetingStream):
    name = 'targeting_device_makes'
    path = "/targeting/device/marketing_name"
    records_jsonpath = "$.targeting_dimensions[*].marketing_name"


class InterestsDLXSTargetingStream(TargetingStream):
    name = 'targeting_interests_dlxs'
    path = "/targeting/interests/dlxs"
    records_jsonpath = "$.targeting_dimensions[*].dlxs"


class InterestsDLXCTargetingStream(TargetingStream):
    name = 'targeting_interests_dlxc'
    path = "/targeting/interests/dlxc"
    records_jsonpath = "$.targeting_dimensions[*].dlxc"


class InterestsDLXPTargetingStream(TargetingStream):
    name = 'targeting_interests_dlxp'
    path = "/targeting/interests/dlxp"
    records_jsonpath = "$.targeting_dimensions[*].dlxp"


class InterestsNLNTargetingStream(TargetingStream):
    name = 'targeting_interests_nln'
    path = "/targeting/interests/nln"
    records_jsonpath = "$.targeting_dimensions[*].nln"


class InterestsPLCTargetingStream(TargetingStream):
    name = 'targeting_interests_plc'
    path = "/targeting/interests/plc"
    records_jsonpath = "$.targeting_dimensions[*].plc"


class LocationCategoriesTargetingStream(TargetingStream):
    name = 'targeting_location_categories'
    path = "/targeting/location/categories_loi"
    records_jsonpath = "$.targeting_dimensions[*].categories_loi"


class TargetingGeoStream(SnapchatAdsStream):
    ignore_parent_replication_key = True
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("targeting_group", th.StringType),
        th.Property("targeting_type", th.StringType),
        th.Property("name", th.StringType),
        th.Property("path", th.StringType),
        th.Property("source", th.StringType),
        th.Property("parent_id", th.StringType),
        th.Property("country_code", th.StringType),
        th.Property("postalCode", th.StringType),
        th.Property("continent", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
            th.Property("full_name", th.StringType),
        )),
        th.Property("country", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
            th.Property("code", th.StringType),
            th.Property("code2", th.StringType),
        )),
        th.Property("region", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
            th.Property("code", th.StringType),
        )),
        th.Property("metro", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
            th.Property("regions", th.StringType),
        )),
        th.Property("city", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
        )),
    ).to_dict()


class CountriesTargetingGeoStream(TargetingGeoStream):
    name = 'targeting_countries'
    path = "/targeting/geo/country"
    records_jsonpath = "$.targeting_dimensions[*].country"
    primary_keys = ["id"]

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        row["id"] = row['country']['id']
        return row


class TargetingGeoStreamMultiCountry(TargetingGeoStream):

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.
        Each record emitted should be a dictionary of property names to their values.
        Args:
            context: Stream partition or context dictionary.
        Yields:
            One item per (possibly processed) record in the API.
        """
        for country_code in self.config["targeting_country_codes"]:
            if context is None:
                context = {}
            context["country_code"] = country_code
            for record in self.request_records(context):
                transformed_record = self.post_process(record, context)
                if transformed_record is None:
                    # Record filtered out during post_process()
                    continue
                yield transformed_record


class RegionsTargetingGeoMultiCountryStream(TargetingGeoStreamMultiCountry):
    name = 'targeting_regions'
    path = "/targeting/geo/{country_code}/region"
    records_jsonpath = "$.targeting_dimensions[*].region"
    primary_keys = ["id", "country_code"]

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        row["id"] = row['region']['id']
        row['country_code'] = context['country_code']
        return row


class MetrosTargetingGeoMultiCountryStream(TargetingGeoStreamMultiCountry):
    name = 'targeting_metros'
    path = "/targeting/geo/{country_code}/metro"
    records_jsonpath = "$.targeting_dimensions[*].metro"
    primary_keys = ["id", "country_code"]

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        row["id"] = row['metro']['id']
        row['country_code'] = context['country_code']
        return row


class PostalCodesTargetingGeoMultiCountryStream(TargetingGeoStreamMultiCountry):
    name = 'targeting_postal_codes'
    path = "/targeting/geo/{country_code}/postal_code"
    records_jsonpath = "$.targeting_dimensions[*].postal_code"
    primary_keys = ["id", "country_code"]

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        row["id"] = row["postalCode"]
        row['country_code'] = context['country_code']
        return row
