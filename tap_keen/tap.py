"""keen tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_keen import streams


class Tapkeen(Tap):
    """keen tap class."""

    name = "tap-keen"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "keen_project_id",
            th.StringType,
            required=True,
            secret=False,
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "keen_read_key",
            th.StringType,
            required=True,
            secret=True,
            description="Project IDs to replicate",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            description="The latest record date to sync",
        ),
        th.Property(
            "max_fetch_interval",
            th.IntegerType,
            default=1,
            description="Max number of hours of data to fetch in one run. Applies to customers stream only",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.keenStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.VideoTimeStream(self),
        ]


if __name__ == "__main__":
    Tapkeen.cli()
