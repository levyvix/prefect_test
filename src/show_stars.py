from datetime import timedelta
from pprint import pprint
from typing import List

import httpx
from prefect import flow, task  # Prefect flow and task decorators
from prefect.cache_policies import INPUTS
from prefect.concurrency.sync import rate_limit
from prefect.futures import PrefectFuture
from pydantic import BaseModel


class GithubRepoStats(BaseModel):
    repo: str
    task: PrefectFuture

    class Config:
        arbitrary_types_allowed = True


@flow(log_prints=True)
def show_stars(github_repos: list[str]):
    """Flow: Show the number of stars that GitHub repos have"""

    repo_stats: List[GithubRepoStats] = []
    for repo in github_repos:
        # Apply the concurrency limit to this loop
        rate_limit("github-api")

        repo_stats.append(
            GithubRepoStats.model_validate(
                {"repo": repo, "task": fetch_stats.submit(repo)}
            )
        )

    for repo_stat in repo_stats:
        repo_name: str = repo_stat.repo
        stars = get_stars(repo_stat.task.result())

        # Print the result
        print(f"{repo_name}: {stars} stars")


@task(cache_policy=INPUTS, cache_expiration=timedelta(minutes=15), retries=3)
def fetch_stats(github_repo: str):
    """Task 1: Fetch the statistics for a GitHub repo"""

    api_response = httpx.get(f"https://api.github.com/repos/{github_repo}")
    api_response.raise_for_status()
    return api_response.json()


@task(log_prints=True)
def get_stars(repo_stats: dict):
    """Task 2: Get the number of stars from GitHub repo statistics"""

    return repo_stats["stargazers_count"]


# Run the flow
if __name__ == "__main__":
    show_stars(["PrefectHQ/prefect", "pydantic/pydantic", "huggingface/transformers"])
