import httpx
from prefect import flow, task


@flow(log_prints=True)
def show_stars(github_repos: list[str]):
    """Show the number of stars that github repos have"""

    for repo in github_repos:
        repo_stats: dict = fetch_stats(repo)
        stars: int = get_stars(repo_stats)
        print(f"{repo} has {stars} stars")


@task()
def fetch_stats(git_repo: str) -> dict:
    """Fetch the statistics for a github repo"""

    return httpx.get(f"https://api.github.com/repos/{git_repo}").json()


@task
def get_stars(repo_stats: dict) -> int:
    """Get the number of stars from the repo statistics"""
    return repo_stats["stargazers_count"]


if __name__ == "__main__":
    show_stars(["PrefectHQ/prefect", "pydantic/pydantic", "huggingface/transformers"])
