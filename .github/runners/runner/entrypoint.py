#!/usr/bin/env python3
import os
import socket
import subprocess
from pathlib import Path


HOST_HOSTNAME_PATH = Path("/etc/host-hostname")


def normalize_hostname(value, source):
    hostname = value.strip().split(".", 1)[0]
    if not hostname or hostname == "unknown-host":
        raise ValueError(f"{source} does not contain a usable hostname")
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
    if any(character not in allowed for character in hostname):
        raise ValueError(f"{source} contains an invalid hostname: {hostname!r}")
    return hostname


def resolve_host_hostname(explicit=None, hostname_path=HOST_HOSTNAME_PATH):
    explicit = os.environ.get("RUNNER_HOSTNAME", "") if explicit is None else explicit
    if explicit.strip() and explicit.strip() != "unknown-host":
        return normalize_hostname(explicit, "RUNNER_HOSTNAME")

    try:
        persisted = hostname_path.read_text(encoding="utf-8")
    except OSError as error:
        raise ValueError(
            f"Set RUNNER_HOSTNAME or mount the host /etc/hostname at {hostname_path}"
        ) from error
    return normalize_hostname(persisted, str(hostname_path))


def runner_instance_hostname():
    container_hostname = socket.gethostname()
    try:
        container_ip = socket.gethostbyname(container_hostname)
        result = subprocess.run(
            ["dig", "-x", container_ip, "+short"],
            check=True,
            capture_output=True,
            text=True,
        )
        reverse_dns_name = result.stdout.strip()
    except (OSError, socket.gaierror, subprocess.CalledProcessError):
        reverse_dns_name = ""
    return normalize_hostname(
        reverse_dns_name or container_hostname,
        "container hostname",
    )


def ensure_docker_storage(
    docker_data_dir,
    runner_hostname,
    docker_root=Path("/var/lib/docker"),
):
    storage = Path(docker_data_dir) / runner_hostname
    storage.mkdir(parents=True, exist_ok=True)

    if docker_root.is_symlink():
        if docker_root.resolve() != storage.resolve():
            raise RuntimeError(
                f"{docker_root} points to {docker_root.resolve()}, not {storage}"
            )
        return storage

    if docker_root.exists():
        if not docker_root.is_dir() or any(docker_root.iterdir()):
            raise RuntimeError(
                f"Refusing to replace non-empty Docker data path {docker_root}"
            )
        docker_root.rmdir()

    docker_root.symlink_to(storage, target_is_directory=True)
    return storage


def create_registration_token(token, org_name, repo_name):
    import requests

    url = f"https://api.github.com/repos/{org_name}/{repo_name}/actions/runners/registration-token"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    response = requests.post(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()["token"]


def main():
    # ensure all variables are set, error message should include unset variables
    unset_vars = [
        var
        for var in [
            "GITHUB_RUNNER_TOKEN",
            "ORGANIZATION_NAME",
            "REPOSITORY_NAME",
            "RUNNER_LABELS",
            "DOCKER_DATA_DIR",
        ]
        if not os.environ.get(var)
    ]
    if unset_vars:
        raise ValueError(f"Variables {', '.join(unset_vars)} must be set")

    runner_hostname = runner_instance_hostname()
    host_hostname = resolve_host_hostname()

    gh_auth_token = os.environ["GITHUB_RUNNER_TOKEN"]
    runner_name = f"{host_hostname}-{runner_hostname}"
    org_name = os.environ["ORGANIZATION_NAME"]
    repo_name = os.environ["REPOSITORY_NAME"]
    runner_labels = os.environ["RUNNER_LABELS"]
    docker_data_dir = os.environ["DOCKER_DATA_DIR"]

    # allow running as root
    os.environ["RUNNER_ALLOW_RUNASROOT"] = "1"

    # starting docker - each runner replica needs a unique /var/lib/docker directory
    # compose isn't capable of editing mounts after startup, so we need to create
    # the directory and symlink it to /var/lib/docker
    ensure_docker_storage(docker_data_dir, runner_hostname)
    subprocess.run(["service", "docker", "start"], check=True)

    # create a registration token for the runner
    registration_token = create_registration_token(gh_auth_token, org_name, repo_name)
    print(f"Configuring the runner with name {runner_name}")
    subprocess.run(
        [
            "/actions-runner/config.sh",
            "--url",
            f"https://github.com/{org_name}/{repo_name}",
            "--token",
            registration_token,
            "--name",
            runner_name,
            "--replace",
            "--labels",
            runner_labels,
            "--unattended",
        ],
        check=True,
    )

    print("Starting the runner...")
    os.execv("/bin/bash", ["/bin/bash", "/actions-runner/run.sh"])


if __name__ == "__main__":
    main()
