#!/usr/bin/env python3
import os
import socket
import subprocess
import time
from pathlib import Path


HOST_HOSTNAME_PATH = Path("/etc/host-hostname")
RUNNER_CONFIG_PATH = Path("/actions-runner/.runner")
RUNNER_CREDENTIALS_PATH = Path("/actions-runner/.credentials")
RUNNER_RSA_PATH = Path("/actions-runner/.credentials_rsaparams")
DOCKER_PID_PATH = Path("/var/run/docker.pid")
DOCKER_READY_TIMEOUT_SECONDS = 60
DOCKER_READY_POLL_SECONDS = 1


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


def process_is_running(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def remove_stale_docker_pid(pid_path=DOCKER_PID_PATH):
    try:
        pid_text = pid_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return False
    except OSError as error:
        raise RuntimeError(f"Unable to read Docker PID file {pid_path}") from error

    try:
        pid = int(pid_text)
    except ValueError:
        pid = 0

    if pid > 0 and process_is_running(pid):
        return False

    print(f"Removing stale Docker PID file {pid_path}")
    pid_path.unlink()
    return True


def wait_for_docker(
    timeout_seconds=DOCKER_READY_TIMEOUT_SECONDS,
    poll_seconds=DOCKER_READY_POLL_SECONDS,
):
    deadline = time.monotonic() + timeout_seconds
    last_error = ""

    while True:
        result = subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.returncode == 0:
            return

        last_error = result.stderr.strip()
        if time.monotonic() >= deadline:
            detail = f": {last_error}" if last_error else ""
            raise RuntimeError(
                f"Docker did not become ready within {timeout_seconds} seconds{detail}"
            )
        time.sleep(poll_seconds)


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


def runner_is_configured(
    config_path=RUNNER_CONFIG_PATH,
    credentials_path=RUNNER_CREDENTIALS_PATH,
    rsa_path=RUNNER_RSA_PATH,
):
    configuration_files = (config_path, credentials_path, rsa_path)
    configured_files = [path for path in configuration_files if path.is_file()]
    if configured_files and len(configured_files) != len(configuration_files):
        raise RuntimeError(
            "Runner configuration is incomplete; recreate the container to register "
            "it again"
        )
    return len(configured_files) == len(configuration_files)


def main():
    # ensure all variables are set, error message should include unset variables
    unset_vars = [
        var
        for var in [
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

    runner_name = f"{host_hostname}-{runner_hostname}"
    org_name = os.environ["ORGANIZATION_NAME"]
    repo_name = os.environ["REPOSITORY_NAME"]
    runner_labels = [
        label.strip()
        for label in os.environ["RUNNER_LABELS"].split(",")
        if label.strip()
    ]
    if host_hostname not in runner_labels:
        runner_labels.append(host_hostname)
    runner_labels = ",".join(runner_labels)
    docker_data_dir = os.environ["DOCKER_DATA_DIR"]

    # allow running as root
    os.environ["RUNNER_ALLOW_RUNASROOT"] = "1"

    # starting docker - each runner replica needs a unique /var/lib/docker directory
    # compose isn't capable of editing mounts after startup, so we need to create
    # the directory and symlink it to /var/lib/docker
    ensure_docker_storage(docker_data_dir, runner_hostname)
    remove_stale_docker_pid()
    subprocess.run(["service", "docker", "start"], check=True)
    wait_for_docker()

    if runner_is_configured():
        print(f"Runner {runner_name} is already configured; reusing its credentials")
    else:
        gh_auth_token = os.environ.get("GITHUB_RUNNER_TOKEN")
        if not gh_auth_token:
            raise ValueError("GITHUB_RUNNER_TOKEN must be set for initial registration")
        registration_token = create_registration_token(
            gh_auth_token,
            org_name,
            repo_name,
        )
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
