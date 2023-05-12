
import pandas as pd
import requests
import re

# Enter your GitHub API access token and repository details.
access_token = "your_access_token_here"
owner_username = "owner_username_here"
repo_name = "repo_name_here"

# Read the input file containing GitHub account IDs.
input_file = "input_file_name_here.xlsx"
github_account_id_column = "GITHUB_ACCOUNT_ID_COLUMN_NAME_HERE"
df = pd.read_excel(input_file)
github_account_ids = df[github_account_id_column].tolist()

# Define the desired permission level.
desired_permission_level = "permission_level_here"

# API endpoint to check if a user is a collaborator.
collaborator_check_endpoint = f"https://api.github.com/repos/{owner_username}/{repo_name}/collaborators/{{collaborator}}"

# API endpoint to add a collaborator.
add_collaborator_endpoint = f"https://api.github.com/repos/{owner_username}/{repo_name}/collaborators/{{collaborator}}"

for github_account_id in github_account_ids:
    try:
        # Extract the GitHub username from the input data
        github_username = re.search(r"(?:[^/]*\/){0,2}([^/]*)", github_account_id).group(1)
        if not github_username:
            print(f"Invalid GitHub account ID: {github_account_id}")
            continue
        # Check if the collaborator is already a collaborator
        collaborator_check_url = collaborator_check_endpoint.format(collaborator=github_account_id)
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
                print(f"{github_account_id} is already a collaborator with {current_permission_level} permission level.")
                continue

        # Check if an invitation is pending for the collaborator
        if status_code == 404:
            invitation_check_url = f"https://api.github.com/repos/{owner_username}/{repo_name}/invitations"
            response = requests.get(invitation_check_url, headers=headers)
            status_code = response.status_code

            if status_code == 200:
                invitations = response.json()
                for invitation in invitations:
                    if invitation["invitee"]["login"] == github_account_id:
                        invitation_permission = invitation["permissions"]
                        print(f"{github_account_id}'s invitation is pending to accept with {invitation_permission} permission level.")
                        break
                else:
                    # If collaborator is not a collaborator and there are no pending invitations, send an invitation
                    add_collaborator_url = add_collaborator_endpoint.format(collaborator=github_account_id)
                    data = {"permission": desired_permission_level}
                    response = requests.put(add_collaborator_url, headers=headers, json=data)
                    status_code = response.status_code

                    if status_code == 201:
                        print(f"New invitation sent to {github_account_id} with {desired_permission_level} permission level.")
                    else:
                        print(f"Error sending invitation to {github_account_id}. Status code: {status_code}")
            else:
                print(f"Error checking invitations. Status code: {status_code}")
        else:
            print(f"Error checking collaborator status for {github_account_id}. Status code: {status_code}")

    except Exception as e:
        print(f"Error processing collaborator {github_account_id}: {str(e)}")






