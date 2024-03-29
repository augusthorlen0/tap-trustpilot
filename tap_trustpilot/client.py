"""REST client handling, including TrustpilotStream base class."""

from __future__ import annotations
import sys
from typing import Any, Callable, Iterable
from pathlib import Path
from urllib.parse import urlencode
from functools import cached_property
import typing as t
import uuid

import requests
import copy
import logging
from urllib.parse import parse_qs, urlparse
from singer_sdk._singerlib import Schema
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.helpers._typing import TypeConformanceLevel
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream


if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"
REVIEWS_PER_PAGE = 50


class TrustpilotStream(RESTStream):
    page_number = 1
    previous_visited_urls = []

    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    # records_jsonpath = "$.reviews.[*]"
    # next_page_token_jsonpath = "$.links.[1].href"  # noqa: S105
    websites_checked: list = []
    business_unit_id: str = None

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://api.trustpilot.com/v1"

    @property
    def schema_filepath(self) -> Path | None:
        """Get path to schema file.

        Returns:
            Path to a schema file for the stream or `None` if n/a.
        """
        return SCHEMAS_DIR / f"{self.name}.schema.json"

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="apikey",
            value=self.config.get("api_key", ""),
            location="params",
        )

    def get_url_params(self, context, next_page_token):
        starting_date = self.get_starting_timestamp(context)
        # params["starting_after"] = self.get_starting_replication_key_value(context)

        if next_page_token is None:
            logging.info(f'{next_page_token=} making it 1')
            self.page_number = 1

        if next_page_token in self.previous_visited_urls:
            # A weird behaviour from TrustPilot API where the last page is shown then it will show the previous one
            # hence 2 is added to the page and visited
            self.page_number = int(parse_qs(urlparse(next_page_token).query).get('page')[0]) + 2
            logging.info(f'Last page visited, sending to an empty page.')

        params = {
            "page": self.page_number,
            "perPage": REVIEWS_PER_PAGE,
            "orderBy": "createdat.desc",
            "startDateTime": self.get_starting_replication_key_value(context),
        }

        self.page_number += 1
        if next_page_token is not None:
            self.previous_visited_urls.append(next_page_token)
        return urlencode(params, safe="()")

    def get_url(self, context):
        context = context or {}

        website_url = context["website_url"]
        context["business_unit_id"] = self.get_business_unit_id(website_url)

        return super().get_url(context)

    def get_business_unit_id(self, website_url):
        if website_url in self.websites_checked:
            logging.debug(
                f"Already have for {website_url} and exitsts in {self.websites_checked}"
            )
            return self.business_unit_id

        logging.debug(
            f"Getting {website_url=} and here is the list {self.websites_checked=}"
        )

        api_key = self.config.get("api_key", "")
        self.website_url = self.config.get("website_url", "")
        url = f"{self.url_base}/business-units/find?name={website_url}"

        header = {"apikey": api_key}
        res = requests.get(url, headers=header)
        res.raise_for_status()

        self.business_unit_id = res.json().get("id")

        self.websites_checked.append(website_url)
        logging.info(f"Fetched {self.business_unit_id=} for {website_url}")

        return self.business_unit_id

    @property
    def partitions(self) -> list[dict] | None:
        """Get stream partitions.

        Developers may override this property to provide a default partitions list.

        By default, this method returns a list of any partitions which are already
        defined in state, otherwise None.

        Returns:
            A list of partition key dicts (if applicable), otherwise `None`.
        """

        website_url_list = self.config.get("website_url", "")

        return [{"website_url": x} for x in website_url_list]

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: A raw :class:`requests.Response`

        Yields:
            One item for every item found in the response.
        """

        res_json = response.json()
        # As it sounds bogus id. This is used as primary key in the TrustpilotScores in order to create history.
        res_json["bogus_id"] = str(uuid.uuid4())

        yield from extract_jsonpath(self.records_jsonpath, input=res_json)
