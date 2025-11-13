"""Database cleanup flow for the OSS testbed.

This flow provides flexible cleanup capabilities for old flow runs
using configurable retention policies with optional human or AI approval.
"""

from datetime import timedelta
from typing import Literal

from prefect import flow, task
from prefect.client.orchestration import get_client
from prefect.flow_runs import pause_flow_run
from prefect.input import RunInput
from pydantic import BaseModel, Field

from ._utils import (
    EntityType,
    StateName,
    build_approval_form_description,
    fetch_old_flow_runs,
    log_cleanup_stats,
    log_flow_run_preview,
    perform_deletion,
)
from .agent import cleanup_agent

# approval form markdown template
APPROVAL_FORM_TEMPLATE = """## cleanup approval required

found **{total_found}** flow run(s) older than `{retention_period}` in states:
{states_formatted}

{flow_runs_list}
{more_exist_message}
⚠️ this action cannot be undone"""

# agent approval prompt
AI_APPROVAL_PROMPT = (
    "review this database cleanup operation and decide if it's safe to proceed:\n\n"
    "{description_md}\n\n"
    "current system context:\n"
    "- retention period: {retention_period}\n"
    "- batch size: {batch_size}\n"
    "- total flow runs to delete: {total_found}\n\n"
    "please investigate the system using the prefect mcp tools to determine if this cleanup is safe."
)


class CleanupApproval(RunInput):
    """Approval input for cleanup operations."""

    approved: bool = Field(
        default=False,
        description="⚠️ Approve deletion? This will permanently remove the flow runs listed above.",
    )
    notes: str = Field(
        default="",
        description="Optional notes about why you approved/rejected this cleanup",
    )


class RetentionConfig(BaseModel):
    """Configuration for data retention cleanup."""

    retention_period: timedelta = Field(
        default=timedelta(hours=1),
        description="How long to retain data (e.g., 'PT1H' for 1 hour, 'P1D' for 1 day)",
        json_schema_extra={"position": 0},
    )

    entity_types: list[EntityType] = Field(
        default=[EntityType.FLOW_RUNS],
        description="Types of entities to clean up",
        json_schema_extra={"position": 1},
    )

    states_to_clean: list[StateName] = Field(
        default=[StateName.COMPLETED, StateName.FAILED, StateName.CANCELLED],
        description="Which terminal states to include in cleanup",
        json_schema_extra={"position": 2},
    )

    batch_size: int = Field(
        default=100,
        ge=10,
        le=1000,
        description="Number of items to delete per batch (10-1000)",
        json_schema_extra={"position": 3},
    )

    rate_limit_delay: float = Field(
        default=0.5,
        ge=0.0,
        le=10.0,
        description="Delay in seconds between API calls for rate limiting (0-10s)",
        json_schema_extra={"position": 4},
    )

    dry_run: bool = Field(
        default=True,
        description="If true, only preview what would be deleted without actually deleting",
        json_schema_extra={"position": 5},
    )

    require_approval: bool = Field(
        default=True,
        description="If true, pause for approval before deleting",
        json_schema_extra={"position": 6},
    )

    approval_type: Literal["human", "ai"] = Field(
        default="human",
        description="Type of approval: 'human' for manual review, 'ai' for agent-based decision",
        json_schema_extra={"position": 7},
    )


@task
async def preview_old_flow_runs(config: RetentionConfig) -> dict:
    """Preview flow runs that would be deleted based on retention policy."""
    print(f"Previewing flow runs older than {config.retention_period}")
    print(f"States to clean: {config.states_to_clean}")

    async with get_client() as client:
        flow_runs = await fetch_old_flow_runs(
            client, config.retention_period, config.states_to_clean, config.batch_size
        )

        if not flow_runs:
            print("No flow runs found matching criteria")
            return {"total_found": 0, "flow_runs": []}

        total_count = len(flow_runs)
        print(
            f"Found {'at least ' if len(flow_runs) == config.batch_size else ''}"
            f"{total_count} flow runs that would be deleted"
        )
        log_flow_run_preview(flow_runs)

        # Return flow run details for the approval form
        flow_run_details = [
            {
                "name": fr.name,
                "id": str(fr.id),
                "start_time": str(fr.start_time),
                "state": fr.state.name if fr.state else "Unknown",
            }
            for fr in flow_runs[:10]  # Limit to first 10 for the form
        ]

        return {
            "total_found": total_count,
            "flow_runs": flow_run_details,
            "has_more": len(flow_runs) == config.batch_size,
        }


@flow
async def get_cleanup_approval(
    config: RetentionConfig, preview: dict
) -> tuple[bool, str | None]:
    """Get approval for cleanup operation (human or AI).

    Args:
        config: Retention configuration
        preview: Preview data with flow runs to be deleted

    Returns:
        Tuple of (approved, notes)
    """
    description_md = build_approval_form_description(
        preview,
        config.retention_period,
        config.states_to_clean,
        APPROVAL_FORM_TEMPLATE,
    )

    if config.approval_type == "human":
        print("Waiting for human approval...")
        approval = await pause_flow_run(
            wait_for_input=CleanupApproval.with_initial_data(description=description_md),
            timeout=3600,
        )
        return approval.approved, approval.notes

    else:  # ai approval
        print("Requesting AI agent approval...")
        prompt = AI_APPROVAL_PROMPT.format(
            description_md=description_md,
            retention_period=config.retention_period,
            batch_size=config.batch_size,
            total_found=preview["total_found"],
        )

        result = await cleanup_agent.run(prompt)
        print(f"AI decision: approved={result.output.approved}, confidence={result.output.confidence}")
        print(f"Reasoning: {result.output.reasoning}")

        if result.output.concerns:
            print(f"Concerns: {', '.join(result.output.concerns)}")

        return result.output.approved, result.output.reasoning


@task
async def delete_old_flow_runs(config: RetentionConfig) -> dict[str, int]:
    """Delete flow runs older than the configured retention period."""
    print("Starting deletion of old flow runs...")

    async with get_client() as client:
        return await perform_deletion(
            client,
            config.retention_period,
            config.states_to_clean,
            config.batch_size,
            config.rate_limit_delay,
        )


@flow(name="database-cleanup", log_prints=True)
async def database_cleanup_entry(
    config: RetentionConfig = RetentionConfig(),
) -> dict[str, int]:
    """Clean up old data from the database."""
    print(
        f"Configuration: retention_period={config.retention_period}, dry_run={config.dry_run}"
    )

    stats: dict[str, int] = {"total_deleted": 0, "total_failed": 0}

    if (
        EntityType.FLOW_RUNS not in config.entity_types
        and EntityType.ALL not in config.entity_types
    ):
        log_cleanup_stats(stats)
        return stats

    if config.dry_run:
        preview = await preview_old_flow_runs(config)
        stats["dry_run"] = True
        stats["flow_runs_found"] = preview["total_found"]
        log_cleanup_stats(stats)
        return stats

    preview = await preview_old_flow_runs(config)

    if preview["total_found"] == 0:
        print("No flow runs found to delete")
        log_cleanup_stats(stats)
        return stats

    if config.require_approval:
        approved, notes = await get_cleanup_approval(config, preview)

        if notes:
            print(f"Approval notes: {notes}")

        if not approved:
            print("❌ Cleanup was not approved - operation cancelled")
            stats["cancelled"] = True
            log_cleanup_stats(stats)
            return stats

        print("✅ Approval received - proceeding with deletion")

    result = await delete_old_flow_runs(config)
    stats["flow_runs_deleted"] = result["deleted"]
    stats["flow_runs_failed"] = result["failed"]
    stats["total_deleted"] += result["deleted"]
    stats["total_failed"] += result["failed"]

    log_cleanup_stats(stats)
    return stats
