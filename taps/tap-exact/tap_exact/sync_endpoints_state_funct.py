from typing import Optional
from singer_sdk.helpers._state import reset_state_progress_markers
from singer_sdk.helpers._typing import to_json_compatible
from singer_sdk.exceptions import InvalidStreamSortException


REPLICATION_INCREMENTAL = "INCREMENTAL"
REPLICATION_LOG_BASED = "LOG_BASED"
PROGRESS_MARKERS = "progress_markers"
PROGRESS_MARKER_NOTE = "Note"
SIGNPOST_MARKER = "replication_key_signpost"
STARTING_MARKER = "starting_replication_value"


def finalize_state_progress_markers(stream_or_partition_state: dict) -> Optional[dict]:
    # function for sync endpoints, only change signpost > than timestamp has been removed as timestamp is an integer that can´t be converted to a date
    stream_or_partition_state.pop(STARTING_MARKER, None)
    if PROGRESS_MARKERS in stream_or_partition_state:
        if "replication_key" in stream_or_partition_state[PROGRESS_MARKERS]:
            # Replication keys valid (only) after sync is complete
            progress_markers = stream_or_partition_state[PROGRESS_MARKERS]
            stream_or_partition_state["replication_key"] = progress_markers.pop(
                "replication_key"
            )
            new_rk_value = progress_markers.pop("replication_key_value")
            stream_or_partition_state["replication_key_value"] = new_rk_value
    # Wipe and return any markers that have not been promoted
    return reset_state_progress_markers(stream_or_partition_state)

def increment_state(
    stream_or_partition_state: dict,
    latest_record: dict,
    replication_key: str,
    is_sorted: bool,
) -> None:
    """Update the state using data from the latest record.

    Raises InvalidStreamSortException if is_sorted=True and unsorted
    data is detected in the stream.
    """
    progress_dict = stream_or_partition_state
    if not is_sorted:
        if PROGRESS_MARKERS not in stream_or_partition_state:
            stream_or_partition_state[PROGRESS_MARKERS] = {
                PROGRESS_MARKER_NOTE: "Progress is not resumable if interrupted."
            }
        progress_dict = stream_or_partition_state[PROGRESS_MARKERS]
    old_rk_value = to_json_compatible(progress_dict.get("replication_key_value"))
    new_rk_value = to_json_compatible(latest_record[replication_key])
    if old_rk_value is None or int(new_rk_value) >= int(old_rk_value):
        progress_dict["replication_key"] = replication_key
        progress_dict["replication_key_value"] = new_rk_value
        return

    if is_sorted:
        raise InvalidStreamSortException(
            f"Unsorted data detected in stream. Latest value '{new_rk_value}' is "
            f"smaller than previous max '{old_rk_value}'."
        )