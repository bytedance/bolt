#!/usr/bin/env python3
import os
import requests

def create_registration_token(token, org_name, repo_name):
    url = f"https://api.github.com/repos/{org_name}/{repo_name}/actions/runners/registration-token"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    response = requests.post(url, headers=headers)
    response.raise_for_status()
    return response.json()["token"]

def main():
    # ensure all variables are set, error message should include unset variables
    unset_vars = [
        var
        for var in [
            "GITHUB_RUNNER_TOKEN",
            "RUNNER_NAME",
            "ORGANIZATION_NAME",
            "REPOSITORY_NAME",
            "RUNNER_LABELS",
        ]
        if not os.environ.get(var)
    ]
    if unset_vars:
        raise ValueError(f"Variables {', '.join(unset_vars)} must be set")

    gh_auth_token = os.environ["GITHUB_RUNNER_TOKEN"]
    runner_name = f"{os.environ['RUNNER_HOSTNAME']}-{os.environ['RUNNER_NAME']}"
    org_name = os.environ["ORGANIZATION_NAME"]
    repo_name = os.environ["REPOSITORY_NAME"]
    runner_labels = os.environ["RUNNER_LABELS"]
    
    # allow running as root
    os.environ["RUNNER_ALLOW_RUNASROOT"] = "1"
    
    # starting docker
    os.system("service docker start")
    # create a registration token for the runner
    registration_token = create_registration_token(gh_auth_token, org_name, repo_name)

    print("Configuring the runner...")
    # Execute /actions-runner/config.sh with the token and labels
    config_command = f"/actions-runner/config.sh --url https://github.com/{org_name}/{repo_name} --token {registration_token} --name {runner_name} --replace --labels {runner_labels} --unattended"
    os.system(config_command)
    
     # Start the runner
    print("Starting the runner...")
    os.execv("/bin/bash", ["/bin/bash", "/actions-runner/run.sh"])

if __name__ == "__main__":
    main()
