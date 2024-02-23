"""Trustpilot tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_trustpilot import streams


class TapTrustpilot(Tap):
    """Trustpilot tap class."""

    name = "tap-trustpilot"

    @property
    def config_jsonschema(self) -> dict:
        return th.PropertiesList(
            th.Property(
                "api_key",
                th.StringType,
                required=True,
                secret=True,  # Flag config as protected.
                description="The token to authenticate against the API service",
            ),
            th.Property(
                "website_url",
                th.ArrayType(th.StringType),
                description="A list of all website URLs that Trustpilot reviews should be fetched from",
            ),
        ).to_dict()

    def discover_streams(self) -> list[streams.TrustpilotStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """

        return [
            streams.Reviews(self),
            streams.TrustpilotScores(self),
        ]


if __name__ == "__main__":
    TapTrustpilot.cli()
