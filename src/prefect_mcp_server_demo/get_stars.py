from prefect import flow, task
import httpx


@task
def get_stars(repo: str):
    url = f"https://api.github.com/repos/{repo}"
    count = httpx.get(url).json()["stargazers_count"]
    print(f"{repo} has {count} stars!")


@flow(name="GitHub Stars", log_prints=True)
def github_stars(repos: list[str]):
    get_stars.map(repos)


if __name__ == "__main__":
    github_stars(
        [
            "PrefectHQ/prefect",
            "PrefectHQ/prefect-mcp-server",
            "jlowin/fastmcp",
            "MarshalX/atproto",
        ]
    )
