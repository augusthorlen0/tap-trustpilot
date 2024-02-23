"""Stream type classes for tap-trustpilot."""

from __future__ import annotations

import sys
import typing as t
import logging

from singer_sdk import Tap, typing as th
from singer_sdk._singerlib import Schema  # JSON Schema typing helpers

from tap_trustpilot.client import TrustpilotStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources


# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class Reviews(TrustpilotStream):
    """
    Get Reviews for the given website in the config file
    """

    name = "reviews"
    path = "/business-units/{business_unit_id}/reviews"
    primary_keys = ("id", "business_unit_id")
    records_jsonpath = "$.reviews.[*]"
    next_page_token_jsonpath = "$.links.[1].href"  # noqa: S105


class TrustpilotScores(TrustpilotStream):
    """
    Get Reviews for the given website in the config file
    """

    name = "trustpilot_scores"
    path = "/business-units/find?name={website_url}"
    primary_keys = ("bogus_id",)
    records_jsonpath = "$.[*]"
