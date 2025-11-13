"""Test script to verify the Pydantic AI agent can access Prefect MCP server."""

import asyncio

from prefect_mcp_server_demo.agent import cleanup_agent


async def test_agent_can_query_prefect():
    """Test that the agent can use Prefect MCP tools to query the system."""
    result = await cleanup_agent.run(
        "what flow runs exist in the system? just give me a quick summary of what you can see."
    )
    print(f"\nagent response:\n{result.output}")
    print(
        f"\nconfidence: {result.output.confidence if hasattr(result.output, 'confidence') else 'N/A'}"
    )


if __name__ == "__main__":
    asyncio.run(test_agent_can_query_prefect())
