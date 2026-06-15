from typing import Any, Dict, Optional, List, Union

from tap_exact.client import ExactStream
from singer_sdk.helpers._state import log_sort_error
import datetime
import pendulum
import copy
from singer_sdk.exceptions import InvalidStreamSortException
from tap_exact.sync_endpoints_state_funct import finalize_state_progress_markers, increment_state

REPLICATION_INCREMENTAL = "INCREMENTAL"
REPLICATION_LOG_BASED = "LOG_BASED"
PROGRESS_MARKERS = "progress_markers"
PROGRESS_MARKER_NOTE = "Note"
SIGNPOST_MARKER = "replication_key_signpost"
STARTING_MARKER = "starting_replication_value"


class ExactSyncStream(ExactStream):
    def get_starting_time(self, context):
        state = self.get_context_state(context)
        rep_key = None
        if "replication_key_value" in state.keys():
            rep_key = state["replication_key_value"]
        return rep_key or 1

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params: dict = {}
        timestamp = self.get_starting_time(context)
        if self.select:
            params["$select"] = self.select
        if self.replication_key and timestamp:
            if timestamp == 1:
                date_filter = f"Timestamp gt {timestamp}"
            else:
                date_filter = f"Timestamp gt {timestamp}L"
            params["$filter"] = date_filter
        if next_page_token:
            params["$skiptoken"] = next_page_token
        return params

    def get_starting_timestamp(
        self, context: Optional[dict]
    ) -> Optional[datetime.datetime]:
        value = self.get_starting_replication_key_value(context)
        if not value:
            return None
        else:
            value = pendulum.parse(value)
        if isinstance(value, datetime.date):
            return None
        return value

    def _increment_stream_state(
        self, latest_record: Dict[str, Any], *, context: Optional[dict] = None
    ) -> None:
        state_dict = self.get_context_state(context)
        if latest_record:
            if self.replication_method in [
                REPLICATION_INCREMENTAL,
                REPLICATION_LOG_BASED,
            ]:
                if not self.replication_key:
                    raise ValueError(
                        f"Could not detect replication key for '{self.name}' stream"
                        f"(replication method={self.replication_method})"
                    )
                treat_as_sorted = self.is_sorted
                if not treat_as_sorted and self.state_partitioning_keys is not None:
                    # Streams with custom state partitioning are not resumable.
                    treat_as_sorted = False
                increment_state(
                    state_dict,
                    replication_key="Timestamp",
                    latest_record=latest_record,
                    is_sorted=treat_as_sorted,
                )

    def finalize_state_progress_markers(self, state: Optional[dict] = None) -> None:
        # all logic is the same, only overwritten to use the customized finalize_state_progress_markers function for timestamp
        if state is None or state == {}:
            for child_stream in self.child_streams or []:
                child_stream.finalize_state_progress_markers()

            context: Optional[dict]
            for context in self.partitions or [{}]:
                context = context or None
                state = self.get_context_state(context)
                finalize_state_progress_markers(state)
            return

        finalize_state_progress_markers(state)

    def _sync_records(  # noqa C901  # too complex
        self, context: Optional[dict] = None
    ) -> None:
        record_count = 0
        current_context: Optional[dict]
        context_list: Optional[List[dict]]
        context_list = [context] if context is not None else self.partitions
        selected = self.selected
        # all logic is the same, only overwritten to use the customized finalize_state_progress_markers function for timestamp
        for current_context in context_list or [{}]:
            partition_record_count = 0
            current_context = current_context or None
            state = self.get_context_state(current_context)
            state_partition_context = self._get_state_partition_context(current_context)
            self._write_starting_replication_value(current_context)
            child_context: Optional[dict] = (
                None if current_context is None else copy.copy(current_context)
            )
            for record_result in self.get_records(current_context):
                if isinstance(record_result, tuple):
                    # Tuple items should be the record and the child context
                    record, child_context = record_result
                else:
                    record = record_result
                child_context = copy.copy(
                    self.get_child_context(record=record, context=child_context)
                )
                for key, val in (state_partition_context or {}).items():
                    # Add state context to records if not already present
                    if key not in record:
                        record[key] = val

                # Sync children, except when primary mapper filters out the record
                if self.stream_maps[0].get_filter_result(record):
                    self._sync_children(child_context)
                self._check_max_record_limit(record_count)
                if selected:
                    if (record_count - 1) % self.STATE_MSG_FREQUENCY == 0:
                        self._write_state_message()
                    self._write_record_message(record)
                    try:
                        self._increment_stream_state(record, context=current_context)
                    except InvalidStreamSortException as ex:
                        log_sort_error(
                            log_fn=self.logger.error,
                            ex=ex,
                            record_count=record_count + 1,
                            partition_record_count=partition_record_count + 1,
                            current_context=current_context,
                            state_partition_context=state_partition_context,
                            stream_name=self.name,
                        )
                        raise ex

                record_count += 1
                partition_record_count += 1
            if current_context == state_partition_context:
                # Finalize per-partition state only if 1:1 with context
                finalize_state_progress_markers(state)
        if not context:
            # Finalize total stream only if we have the full full context.
            # Otherwise will be finalized by tap at end of sync.
            finalize_state_progress_markers(self.stream_state)
        self._write_record_count_log(record_count=record_count, context=context)
        # Reset interim bookmarks before emitting final STATE message:
        self._write_state_message()
