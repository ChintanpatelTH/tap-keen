"""REST client handling, including keenStream base class."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import requests
from dateutil import parser
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class KeenStream(RESTStream):
    """keen stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return f"https://api.keen.io/3.0/projects/{self.config.get('keen_project_id')}/"

    records_jsonpath = "$[*]"  # Or override `parse_response`.

    # Set this value or override `get_new_paginator`.
    next_page_token_jsonpath = "$.next_page"  # noqa: S105

    start_date: str | None = None
    end_date: str | None = None

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        headers["Authorization"] = self.config.get("keen_read_key")
        return headers

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        return super().get_new_paginator()

    def get_records(self, context: dict) -> Iterable[dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Args:
            context: The stream context.

        Yields:
            Each record from the source.
        """
        current_state = self.get_context_state(context)
        current_date = parser.parse("2022-03-16T00:00:02.000+00:00")
        date_window_size = float(self.config.get("max_fetch_interval", 1))
        min_value = current_state.get(
            "replication_key_value",
            self.config.get("start_date", ""),
        )
        context = context or {}
        min_date = parser.parse(min_value)
        while min_date < current_date:
            updated_at_max = min_date + timedelta(hours=date_window_size)
            if updated_at_max > current_date:
                updated_at_max = current_date

            self.start_date = min_date.isoformat()
            self.end_date = updated_at_max.isoformat()
            yield from super().get_records(context)
            self._increment_stream_state({"created_at": self.end_date}, context=context)
            self._write_state_message()
            min_date = updated_at_max

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ARG002
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        timeframe_dict = {
            "start": self.start_date,
            "end": self.end_date,
        }
        params["event_collection"] = self.name
        params["timeframe"] = json.dumps(timeframe_dict)
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())
