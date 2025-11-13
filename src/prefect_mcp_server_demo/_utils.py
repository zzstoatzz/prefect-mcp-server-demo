"""Internal utilities for flow operations."""

import asyncio
from datetime import datetime, timedelta, timezone
from enum import Enum

from prefect.client.orchestration import PrefectClient
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterStartTime,
    FlowRunFilterState,
    FlowRunFilterStateName,
)
from prefect.client.schemas.objects import FlowRun


class EntityType(str, Enum):
    """Types of entities that can be cleaned up."""

    FLOW_RUNS = "flow_runs"
    ALL = "all"


class StateName(str, Enum):
    """Terminal state names for flow runs."""

    COMPLETED = "Completed"
    FAILED = "Failed"
    CANCELLED = "Cancelled"
    CRASHED = "Crashed"


async def fetch_old_flow_runs(
    client: PrefectClient,
    retention_period: timedelta,
    states: list[StateName],
    limit: int = 100,
) -> list[FlowRun]:
    """Fetch flow runs older than the retention period."""
    cutoff = datetime.now(timezone.utc) - retention_period
    flow_run_filter = FlowRunFilter(
        start_time=FlowRunFilterStartTime(before_=cutoff),
        state=FlowRunFilterState(
            name=FlowRunFilterStateName(any_=[s.value for s in states])
        ),
    )
    return await client.read_flow_runs(flow_run_filter=flow_run_filter, limit=limit)


def log_flow_run_preview(flow_runs: list[FlowRun], limit: int = 5) -> None:
    """Log a preview of flow runs that will be affected."""
    for i, fr in enumerate(flow_runs[:limit]):
        print(
            f"  Sample {i + 1}: {fr.name} ({fr.id}) "
            f"from {fr.start_time}, state: {fr.state.name if fr.state else 'Unknown'}"
        )
    if len(flow_runs) > limit:
        print(f"  ... and {len(flow_runs) - limit} more in this batch")


async def batch_delete_flow_runs(
    client: PrefectClient,
    flow_runs: list[FlowRun],
    rate_limit_delay: float = 0.5,
) -> tuple[int, int]:
    """Delete flow runs in batches with error handling.

    Returns:
        Tuple of (deleted_count, failed_count)
    """
    deleted_total = 0
    failed_total = 0

    async def delete_with_error_handling(flow_run_id):
        try:
            await client.delete_flow_run(flow_run_id)
            return flow_run_id, None
        except Exception as e:
            return flow_run_id, str(e)

    batch_size = len(flow_runs)
    results = await asyncio.gather(
        *[delete_with_error_handling(fr.id) for fr in flow_runs],
        return_exceptions=False,
    )

    failed_deletes = []
    for flow_run_id, error in results:
        if error:
            failed_deletes.append((flow_run_id, error))
            failed_total += 1
        else:
            deleted_total += 1

    batch_deleted = batch_size - len(failed_deletes)
    print(
        f"Batch complete: deleted {batch_deleted}/{batch_size} "
        f"(total: {deleted_total}, failed: {failed_total})"
    )

    if failed_deletes:
        print(f"Failed to delete {len(failed_deletes)} flow runs in this batch")
        for flow_run_id, error in failed_deletes[:3]:
            print(f"  - {flow_run_id}: {error}")

    if rate_limit_delay > 0:
        await asyncio.sleep(rate_limit_delay)

    return deleted_total, failed_total


def build_approval_form_description(
    preview: dict,
    retention_period: timedelta,
    states_to_clean: list[StateName],
    template: str,
) -> str:
    """Build the markdown description for the approval form.

    Args:
        preview: Preview data with flow_runs list and metadata
        retention_period: The configured retention period
        states_to_clean: List of state names being cleaned
        template: The markdown template string

    Returns:
        Formatted markdown string for the approval form
    """
    states_formatted = "\n".join(f"- `{s.value}`" for s in states_to_clean)

    # Format flow runs list
    flow_runs_lines = []
    for i, fr in enumerate(preview["flow_runs"], 1):
        # Parse and format timestamp more readably
        try:
            dt = datetime.fromisoformat(fr["start_time"].replace("+00:00", ""))
            time_str = dt.strftime("%b %d, %H:%M")
        except (ValueError, AttributeError):
            time_str = fr["start_time"]

        flow_runs_lines.append(f"{i}. **{fr['name']}** · `{fr['state']}` · {time_str}")

    flow_runs_list = "\n".join(flow_runs_lines)
    more_exist_message = (
        "\n_showing first 10, more exist_\n\n" if preview.get("has_more") else ""
    )

    return template.format(
        total_found=preview["total_found"],
        retention_period=retention_period,
        states_formatted=states_formatted,
        flow_runs_list=flow_runs_list,
        more_exist_message=more_exist_message,
    )


def log_cleanup_stats(stats: dict[str, int]) -> None:
    """Log cleanup statistics in a consistent format.

    Args:
        stats: Dictionary of cleanup statistics
    """
    print("Database cleanup finished")
    print("Summary:")
    for key, value in stats.items():
        print(f"  {key}: {value}")


async def perform_deletion(
    client: PrefectClient,
    retention_period: timedelta,
    states_to_clean: list[StateName],
    batch_size: int,
    rate_limit_delay: float,
) -> dict[str, int]:
    """Perform the actual deletion of flow runs.

    Args:
        client: Prefect client
        retention_period: How far back to keep data
        states_to_clean: Which terminal states to clean
        batch_size: Number of items per batch
        rate_limit_delay: Delay between batches

    Returns:
        Dictionary with deletion statistics
    """
    deleted_total = 0
    failed_total = 0
    total_count = 0

    flow_runs = await fetch_old_flow_runs(
        client, retention_period, states_to_clean, batch_size
    )

    if not flow_runs:
        print("No flow runs found to delete")
        return {"deleted": 0, "failed": 0, "total_found": 0}

    total_count = len(flow_runs)

    while flow_runs:
        deleted, failed = await batch_delete_flow_runs(
            client, flow_runs, rate_limit_delay
        )
        deleted_total += deleted
        failed_total += failed

        flow_runs = await fetch_old_flow_runs(
            client, retention_period, states_to_clean, batch_size
        )

    print(f"Cleanup complete. Deleted: {deleted_total}, Failed: {failed_total}")
    return {
        "deleted": deleted_total,
        "failed": failed_total,
        "total_found": total_count,
    }
