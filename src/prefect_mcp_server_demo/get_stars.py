from prefect import flow, task
import httpx


@task
def get_stars(repo: str):
    url = f"https://api.github.com/repos/{repo}"
    count = httpx.get(url).json()["stargazer_count"]
    print(f"{repo} has {count} stars!")


@flow(name="GitHub Stars", log_prints=True)
def github_stars(repos: list[str]):
    for repo in repos:
        get_stars(repo)
