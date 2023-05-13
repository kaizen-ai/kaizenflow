import argparse
import requests

# Enter your GitHub API access token and repository details
access_token = "your_access_token_here"
owner_username = "owner_username_here"
repo_name = "repo_name_here"

# API endpoint to check if a user is a collaborator
collaborator_check_endpoint = f"https://api.github.com/repos/{owner_username}/{repo_name}/collaborators/{{collaborator}}"

# API endpoint to add a collaborator
add_collaborator_endpoint = f"https://api.github.com/repos/{owner_username}/{repo_name}/collaborators/{{collaborator}}"

# Parse command-line arguments for GitHub usernames
parser = argparse.ArgumentParser()
parser.add_argument("usernames", nargs="+", help="GitHub usernames")
args = parser.parse_args()
github_usernames = args.usernames

# Process each GitHub username
for github_username in github_usernames:
    # Check if the collaborator is already a collaborator
    collaborator_check_url = collaborator_check_endpoint.format(collaborator=github_username)
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(collaborator_check_url, headers=headers)
    status_code = response.status_code

    # If collaborator is already a collaborator, get their permission level
    if status_code == 204:
        collaborator_permissions_url = collaborator_check_url + "/permission"
        response = requests.get(collaborator_permissions_url, headers=headers)
        status_code = response.status_code

        if status_code == 200:
            current_permission_level = response.json()["permission"]
            print(f"{github_username} is already a collaborator with {current_permission_level} permission level.")
            continue

    # Check if an invitation is pending for the collaborator
    if status_code == 404:
        invitation_check_url = f"https://api.github.com/repos/{owner_username}/{repo_name}/invitations"
        response = requests.get(invitation_check_url, headers=headers)
        status_code = response.status_code

        if status_code == 200:
            invitations = response.json()
            for invitation in invitations:
                invitee_login = invitation["invitee"]["login"]
                invitee_permission = invitation["permissions"]
                if invitee_login == github_username:
                    print(f"{github_username}'s invitation is pending to accept with {invitee_permission} permission level.")
                    break
            else:
                # If collaborator is not a collaborator and there are no pending invitations, send an invitation
                add_collaborator_url = add_collaborator_endpoint.format(collaborator=github_username)
                data = {"permission": "desired_permission_level_here"}
                response = requests.put(add_collaborator_url, headers=headers, json=data)
                status_code = response.status_code

                if status_code == 201:
                    print(f"New invitation sent to {github_username} with the desired permission level.")
    else:
        print(f"Error retrieving information for {github_username}. Status code: {status_code}")
