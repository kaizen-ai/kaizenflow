#!/usr/bin/env python

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

import argparse
import logging

import requests

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--github_username",
        type=str,
        required=True,
        help="Github username that you are providing the permissions",
    )
    parser.add_argument(
        "--owner_username",
        type=str,
        required=True,
        help="Owner's username of the repository",
    )
    parser.add_argument(
        "--repo_name",
        type=str,
        required=True,
        help="Repository name",
    )
    parser.add_argument(
        "--access_token",
        type=str,
        required=True,
        help="Owner's generated access token",
    )
    return parser.parse_args()


def check_collaborator(
    owner_username: str,
    repo_name: str,
    access_token: str,
    github_username: str,
) -> None:

    add_collaborator_endpoint = (
        f"https://api.github.com/repos/{owner_username}/{repo_name}/"
        f"collaborators/{{collaborator}}"
    )

    collaborator_check_url = (
        f"https://api.github.com/repos/{owner_username}/{repo_name}/"
        f"collaborators/{github_username}"
    )

    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(collaborator_check_url, headers=headers, timeout=10)
    status_code = response.status_code

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
            _LOG.debug(
                "%s is already a collaborator with %s permission level.",
                github_username,
                current_permission_level,
            )

        else:
            _LOG.debug(
                "Error retrieving permission level for %s. Status code: %s",
                github_username,
                status_code,
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
            # If collaborator is already a collaborator, get their permission level.
            invitations = response.json()
            for invitation in invitations:
                invitee_login = invitation["invitee"]["login"]
                invitee_permission = invitation["permissions"]
                if invitee_login == github_username:
                    _LOG.debug(
                        "%s's invitation is pending to accept with %s permission level.",
                        github_username,
                        invitee_permission,
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
                data = {"permission": "pull"}
                response = requests.put(
                    add_collaborator_url, headers=headers, json=data, timeout=10
                )
                status_code = response.status_code

                if status_code == 201:
                    _LOG.debug(
                        "New invitation sent to %s with permission level.",
                        github_username,
                    )
                else:
                    _LOG.debug(
                        "Error sending invitation to %s. Status code: %s",
                        github_username,
                        status_code,
                    )

        else:
            _LOG.debug(
                "Error retrieving invitations for %s/%s. Status code: %s Permission level: %s",
                owner_username,
                repo_name,
                status_code,
                current_permission_level,
            )

    else:
        _LOG.debug(
            "Error retrieving information for %s. Status code: %s",
            github_username,
            status_code,
        )


def main() -> None:
    args = _parse()

    owner_username = args.owner_username
    repo_name = args.repo_name
    access_token = args.access_token

    check_collaborator(
        owner_username,
        repo_name,
        access_token,
        args.github_username,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
