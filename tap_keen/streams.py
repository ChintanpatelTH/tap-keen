"""Stream type classes for tap-keen."""

from __future__ import annotations

from pathlib import Path

from tap_keen.client import KeenStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class VideoTimeStream(KeenStream):
    """Define video time update stream."""

    name = "video_time_updates"
    path = "queries/extraction"
    records_jsonpath = "$.result[*]"
    primary_keys = ["keen.id"]  # noqa: RUF012
    replication_key = "created_at"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "report.json"
    is_sorted = True

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        # Currently this is a workaround to have nested replication keys
        row["created_at"] = row["keen"].pop("created_at")
        return row
