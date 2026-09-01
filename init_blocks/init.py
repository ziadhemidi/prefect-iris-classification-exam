"""
Initialize Prefect blocks and variables.
This is a boilerplate that needs to be completed as part of the challenge.
"""

import os
import asyncio
from dotenv import load_dotenv
from prefect_github import GitHubCredentials, GitHubRepository
from prefect.variables import Variable

async def create_blocks():
    """Initialize all required Prefect blocks."""
    # TODO: Implement block creation
    # 1. Load environment variables using load_dotenv()
    load_dotenv()  # Load environment variables
    
    # 2. Create GitHub credentials block:
    #    - Get token from GITHUB_TOKEN environment variable
    #    - Create GitHubCredentials with token
    #    - Save with name="github-credentials"
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        raise ValueError("GITHUB_TOKEN environment variable not set")
    credentials = GitHubCredentials(token=github_token)
    await credentials.save(name="github-credentials", overwrite=True)
    
    # 3. Create GitHub repository block:
    #    - Get repository URL from GITHUB_REPOSITORY environment variable
    #    - Create GitHubRepository with:
    #      * repository_url from step 2
    #      * reference from GITHUB_BRANCH environment variable
    #      * credentials from step 1
    #    - Save with name="github-repo"
    repository_url = os.getenv("GITHUB_REPOSITORY")
    if not repository_url:
        raise ValueError("GITHUB_REPOSITORY environment variable not set")
    github_repo = GitHubRepository(
        repository_url=f"https://github.com/{repository_url}",
        reference=os.getenv("GITHUB_BRANCH", "main"),  # default to main if not specified
        credentials=credentials
    )
    await github_repo.save(name="github-repo", overwrite=True)
    
    # 4. Create default variables:
    await Variable.set("rf_n_estimators", "100", overwrite=True)
    await Variable.set("rf_max_depth", "10", overwrite=True)
    await Variable.set("rf_test_size", "0.2", overwrite=True)
    await Variable.set("model_name", "iris_model", overwrite=True)
    await Variable.set("model_version", "1", overwrite=True)

if __name__ == "__main__":
    asyncio.run(create_blocks())