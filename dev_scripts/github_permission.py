#!/usr/bin/env python.
"""
This script checks if a GitHub user is already a collaborator of a specific repository,
sends an invitation if not, and reports any pending invitations.

Example:
    ```
    > dev_scripts/github_permission.py \
        --owner_username gpsaggese \
        ...
    ```

Import as:

import dev_scripts.github_permission as descgipe
"""
import argparse
import requests
# Enter your GitHub API access token and repository details.
ACCESS_TOKEN = "YOUR_ACCESS_TOKEN_HERE"
OWNER_USERNAME = "OWNER_USERNAME_HERE"
REPO_NAME = "REPO_NAME_HERE"
# API endpoint to check if a user is a collaborator.
collaborator_check_endpoint = (
    f"https://api.github.com/repos/{OWNER_USERNAME}/{REPO_NAME}/collaborators/{{collaborator}}"
)
# API endpoint to add a collaborator.
add_collaborator_endpoint = (
    f"https://api.github.com/repos/{OWNER_USERNAME}/{REPO_NAME}/collaborators/{{collaborator}}"
)
def log_message(message):
    """Print log message."""
    print(message)
def check_collaborator(github_username):
    """Check if a GitHub user is already a collaborator."""
    collaborator_check_url = collaborator_check_endpoint.format(collaborator=github_username)
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    response = requests.get(collaborator_check_url, headers=headers, timeout=10)
    status_code = response.status_code
    # If collaborator is already a collaborator, get their permission level.
    if status_code == 204:
        collaborator_permissions_url = collaborator_check_url + "/permission"
        response = requests.get(collaborator_permissions_url, headers=headers, timeout=10)
        status_code = response.status_code

        if status_code == 200:
            current_permission_level = response.json()["permission"]
            log_message(
                f"{github_username} is already a collaborator with "
                f"{current_permission_level} permission level."
            )
            return
    # Check if an invitation is pending for the collaborator.
    if status_code == 404:
        invitation_check_url = (
            f"https://api.github.com/repos/{OWNER_USERNAME}/"
            f"{REPO_NAME}/invitations"
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
                    return
        # If collaborator is not a collaborator and there are no pending invitations,
        # send an invitation.
        add_collaborator_url = add_collaborator_endpoint.format(
            collaborator=github_username
        )
        data = {"permission": "desired_permission_level_here"}
        response = requests.put(
            add_collaborator_url, headers=headers, json=data, timeout=10
        )
        status_code = response.status_code

        if status_code == 201:
            log_message(f"New invitation sent to {github_username} with permission level.")
            return
    log_message(f"Error retrieving information for {github_username}. Status code: {status_code}")
def main():
    """Main function."""
    # Parse command-line arguments for GitHub usernames.
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--usernames",
        action="store",
        required=True,
        default=None,
        help="Space-separated GitHub usernames to check and invite",
    )
    args = parser.parse_args()
    github_usernames = args.usernames.split()
    # Process each GitHub username.
    for github_username in github_usernames:
        check_collaborator(github_username)
if __name__ == "__main__":
    main()
