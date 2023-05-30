"""
The script checks if a GitHub user is already a collaborator of a specific
repository, sends an invitation if not, and reports any pending invitations.

Example:
    ```
    >python dev_scripts/github_permission.py GITHUB_USERNAME\
        ...
    ```

Import as:

import dev_scripts.github_permission as descgipe
"""

#!/usr/bin/env python
import argparse
import logging

import requests

_LOG = logging.getLogger(__name__)


def log_message(message: str) -> None:
    """
    Log a message.
    """
    _LOG.debug(message)


def check_collaborator(
    owner_username: str,
    repo_name: str,
    access_token: str,
    github_username: str,
) -> None:
    """
    Check if a GitHub user is already a collaborator.
    """
    collaborator_check_endpoint: str = (
        "https://api.github.com/repos/{owner_username}/{repo_name}/collaborators/"
        "{{collaborator}}"
    )
    add_collaborator_endpoint: str = (
        "https://api.github.com/repos/{owner_username}/{repo_name}/collaborators/"
        "{{collaborator}}"
    )

    collaborator_check_url = collaborator_check_endpoint.format(
        owner_username=owner_username,
        repo_name=repo_name,
        collaborator=github_username,
    )
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(collaborator_check_url, headers=headers, timeout=10)
    status_code = response.status_code

    # If collaborator is already a collaborator, get their permission level.
    if status_code == 204:
        collaborator_permissions_url = "/".join(
            [collaborator_check_url, "permission"]
        )
        response = requests.get(
            collaborator_permissions_url, headers=headers, timeout=10
        )
        status_code = response.status_code

        if status_code == 200:
            current_permission_level = response.json()["permission"]
            log_message(
                f"{github_username} is already a collaborator with "
                f"{current_permission_level} permission level."
            )
        else:
            log_message(
                f"Error retrieving permission level for {github_username}. "
                f"Status code: {status_code}"
            )

    # Check if an invitation is pending for the collaborator.
    elif status_code == 404:
        invitation_check_url = (
            f"https://api.github.com/repos/{owner_username}/"
            f"{repo_name}/invitations"
        )
        response = requests.get(invitation_check_url, headers=headers, timeout=10)
        status_code = response.status_code

        if status_code == 200:
            invitations = response.json()
            for invitation in invitations:
                invitee_login = invitation["invitee"]["login"]
                invitee_permission = invitation["permissions"]
                if invitee_login == github_username:
                    log_message(
                        f"{github_username}'s invitation is pending to accept with"
                        f" {invitee_permission} permission level."
                    )
                    break
            else:
                # If collaborator is not a collaborator and there are no pending invitations,
                # send an invitation.
                add_collaborator_url = add_collaborator_endpoint.format(
                    owner_username=owner_username,
                    repo_name=repo_name,
                    collaborator=github_username,
                )
                data = {"permission": "desired_permission_level_here"}
                response = requests.put(
                    add_collaborator_url, headers=headers, json=data, timeout=10
                )
                status_code = response.status_code

                if status_code == 201:
                    log_message(
                        f"New invitation sent to {github_username} with permission level."
                    )
                else:
                    log_message(
                        f"Error sending invitation to {github_username}. Status code: {status_code}"
                    )

        else:
            log_message(
                f"Error retrieving invitations for {owner_username}/{repo_name}. "
                f"Status code: {status_code}"
            )

    else:
        log_message(
            f"Error retrieving information for {github_username}. Status code: {status_code}"
        )


def main() -> None:
    """
    Perform the main execution logic of the script.
    """
    # Parse command-line arguments for GitHub usernames.
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--owner-username",
        action="store",
        required=True,
        help="Owner's GitHub username",
    )
    parser.add_argument(
        "--repo-name", 
        action="store", 
        required=True, 
        help="Repository name"
    )
    parser.add_argument(
        "--access-token",
        action="store",
        required=True,
        help="GitHub access token",
    )
    parser.add_argument(
        "--github-username",
        action="store",
        required=True,
        help="GitHub username to check",
    )
    args = parser.parse_args()

    # Check collaborator status for the provided GitHub username.
    check_collaborator(
        args.owner_username,
        args.repo_name,
        args.access_token,
        args.github_username,
    )


if __name__ == "__main__":
    # Configure logging settings
    logging.basicConfig(level=logging.DEBUG)
    main()
