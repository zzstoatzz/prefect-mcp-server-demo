"""Pydantic AI agent for autonomous Prefect infrastructure decisions."""

from pydantic import BaseModel, Field
from pydantic_ai import Agent
from pydantic_ai.durable_exec.prefect import PrefectAgent, TaskConfig
from pydantic_ai.mcp import MCPServerStdio
from pydantic_ai.models.anthropic import AnthropicModel
from pydantic_ai.providers.anthropic import AnthropicProvider
from pydantic_settings import BaseSettings, SettingsConfigDict


class PrefectMCPSettings(BaseSettings):
    """Settings for the Prefect MCP server."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    command: str = "uvx"
    args: list[str] = Field(
        default_factory=lambda: [
            "--from",
            "prefect-mcp",
            "prefect-mcp-server",
        ]
    )


class Settings(BaseSettings):
    """Settings for the cleanup safety agent."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    anthropic_api_key: str = Field(default=...)

    prefect_mcp_settings: PrefectMCPSettings = Field(default_factory=PrefectMCPSettings)


settings = Settings()

# system prompt for cleanup safety agent
CLEANUP_AGENT_PROMPT = (
    "you are a database cleanup safety agent for a Prefect orchestration system. "
    "your job is to review proposed cleanup operations and decide if they're safe to proceed.\n\n"
    "consider:\n"
    "- are there any recently active flows that would be affected?\n"
    "- is the retention period reasonable given the current state?\n"
    "- are there any patterns suggesting important data in the deletion target?\n"
    "- what is the overall system health and activity level?\n\n"
    "use the Prefect MCP server tools to investigate:\n"
    "- recent flow run activity and patterns\n"
    "- deployment schedules and status\n"
    "- historical cleanup patterns\n"
    "- system state and health\n\n"
    "be conservative but practical. if you're uncertain, explain your concerns clearly. "
    "your confidence score should reflect how certain you are about the decision."
)


class CleanupDecision(BaseModel):
    """Decision about whether to proceed with cleanup."""

    approved: bool
    confidence: float  # 0-1 confidence score
    reasoning: str
    concerns: list[str] | None = None


# create the Prefect MCP server toolset using uvx
prefect_mcp: MCPServerStdio = MCPServerStdio(
    **settings.prefect_mcp_settings.model_dump()
)

# wrap agent with Prefect for durability and observability
cleanup_agent: PrefectAgent[None, CleanupDecision] = PrefectAgent(
    Agent(
        model=AnthropicModel(
            model_name="claude-haiku-4-5",
            provider=AnthropicProvider(api_key=settings.anthropic_api_key),
        ),
        output_type=CleanupDecision,
        system_prompt=CLEANUP_AGENT_PROMPT.strip(),
        toolsets=[prefect_mcp],
    ),
    name="cleanup-safety-agent",
    model_task_config=TaskConfig(
        retries=3,
        retry_delay_seconds=[1.0, 2.0, 4.0],
        timeout_seconds=60.0,
    ),
    tool_task_config=TaskConfig(
        retries=2,
        retry_delay_seconds=[0.5, 1.0],
    ),
)
