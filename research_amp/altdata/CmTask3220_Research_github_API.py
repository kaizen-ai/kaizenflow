# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Description

# %% [markdown]
#  This notebook researches the abilities of GitHub API. 
#  https://docs.github.com/en/rest?apiVersion=2022-11-28
#  
# Obviously, the actual list of github API endpoints is much longer than presented in this notebook. 
# The data from the endpoints presented here are assumed to have the biggest predictive power.

# %% [markdown]
# # Imports

# %%
import logging
import requests

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Common repository info

# %%
common = requests.get("https://api.github.com/repos/bitcoin/bitcoin").json()

# %%
common

# %% [markdown]
# # Stars

# %% [markdown]
# Get the current number of stars for the repository.

# %% run_control={"marked": false}
common["stargazers_count"]

# %% [markdown]
# # Commits

# %% [markdown]
# ## /commit_activity

# %% [markdown]
# Returns the last year of commit activity grouped by week. The days array is a group of commits per day, starting on Sunday.
#
#

# %%
commits_yearly = requests.get("https://api.github.com/repos/bitcoin/bitcoin/stats/commit_activity").json()

# %% [markdown]
# E.g. in the array [8, 11, 10, 25, 5, 13, 2] 8 is the number of commits for Sun, 11 - for Monday, 10 - for Tuesday, 25 - for Wednesday, 5 - for Thursday, 13 - for Friday and 2 - for Saturday

# %%
commits_yearly[:5]

# %% [markdown]
# ## /code_frequency

# %% [markdown]
# Returns a historical weekly aggregate of the number of additions and deletions pushed to a repository.

# %%
all_commits_weekly_aggregated = requests.get("https://api.github.com/repos/bitcoin/bitcoin/stats/code_frequency").json()

# %%
# First date Sun Aug 30 2009 00:00:00 GMT+0000, but common info says that repository was created on '2010-12-19T15:16:43Z'
# How is it possible?
print("First five weeks:", commits_weekly_aggregated[:5])
print("Last five weeks:", commits_weekly_aggregated[-5:])

# %% [markdown]
# ## /participation

# %% [markdown]
# Returns the total commit counts for the owner and total commit counts in all. all is everyone combined, including the owner in the last 52 weeks. If you'd like to get the commit counts for non-owners, you can subtract owner from all.
#
# The array order is oldest week (index 0) to most recent week.

# %%
total_commits = requests.get("https://api.github.com/repos/bitcoin/bitcoin/stats/participation").json()

# %%
print(total_commits)

# %% [markdown]
# ## /punch_card

# %% [markdown]
# Get the hourly commit count for each day of the last week.
#

# %% [markdown]
# Each array contains the day number, hour number, and number of commits:
#
# 0-6: Sunday - Saturday
# 0-23: Hour of day
# Number of commits
# For example, [2, 14, 25] indicates that there were 25 total commits, during the 2:00pm hour on Tuesdays. All times are based on the time zone of individual commits.

# %%
hourly_commits = requests.get("https://api.github.com/repos/bitcoin/bitcoin/stats/punch_card").json()

# %%
hourly_commits

# %% [markdown]
# # ISSUES

# %% [markdown]
# List issues in a repository. Only open issues will be listed.
#
# Note: GitHub's REST API considers every pull request an issue, but not every issue is a pull request. For this reason, "Issues" endpoints may return both issues and pull requests in the response. You can identify pull requests by the pull_request key. Be aware that the id of a pull request returned from "Issues" endpoints will be an issue id. To find out the pull request id, use the "List pull requests" endpoint.

# %%
issues = requests.get("https://api.github.com/repos/bitcoin/bitcoin/issues").json()

# %%
len(issues)

# %% [markdown]
# # Search

# %% [markdown]
# The Search API has a custom rate limit. For requests using Basic Authentication, OAuth, or client ID and secret, you can make up to 30 requests per minute. For unauthenticated requests, the rate limit allows you to make up to 10 requests per minute.

# %% [markdown]
# ## Repositories

# %%
query = "blockchain"
search_repos = requests.get(f"https://api.github.com/search/repositories?q={query}").json()

# %%
search_repos["total_count"]

# %%
# First result for "blockchain" search query.
search_repos["items"][:1]

# %% [markdown]
# # Rate Limit

# %% [markdown]
# The Search API has a custom rate limit, separate from the rate limit governing the rest of the REST API. The GraphQL API also has a custom rate limit that is separate from and calculated differently than rate limits in the REST API.
#
# For these reasons, the Rate Limit API response categorizes your rate limit. Under resources, you'll see four objects:
#
# The core object provides your rate limit status for all non-search-related resources in the REST API.
#
# The search object provides your rate limit status for the Search API.
#
# The graphql object provides your rate limit status for the GraphQL API.
#
# The integration_manifest object provides your rate limit status for the GitHub App Manifest code conversion endpoint.

# %%
requests.get("https://api.github.com/rate_limit").json()

# %%
