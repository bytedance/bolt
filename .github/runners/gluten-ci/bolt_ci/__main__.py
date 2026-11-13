# Copyright (c) ByteDance Ltd. and/or its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Command line entry points for the external CI services."""

import argparse
import hmac
import json
import logging
import os
import signal
import sqlite3
import subprocess
import tempfile
import threading
from pathlib import Path

from .commenter import Commenter
from .config import Config
from .docker import Docker, DockerError, GIT_BIN, PROCESS_ENV, Workspaces
from .github import ApiError, GitHub, positive
from .listener import Listener
from .runner import Runner
from .state import State


def parser():
    root = argparse.ArgumentParser(description="Gluten Bolt external CI")
    root.add_argument("--config", default=os.environ.get("BOLT_CI_CONFIG"))
    subcommands = root.add_subparsers(dest="command", required=True)
    for command in ("listener", "runner", "commenter", "status", "enqueue", "prepare"):
        child = subcommands.add_parser(command)
        child.add_argument("--config", default=argparse.SUPPRESS)
        if command in {"listener", "runner", "commenter"}:
            child.add_argument("--once", action="store_true")
        if command == "enqueue":
            child.add_argument("--pr", required=True, type=int)
            child.add_argument("--dry-run", action="store_true")
    return root


def _revision(checkout, expected):
    env = PROCESS_ENV | {"GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null"}
    try:
        actual = (
            subprocess.check_output(
                [
                    GIT_BIN,
                    "-c",
                    f"safe.directory={checkout}",
                    "-C",
                    str(checkout),
                    "rev-parse",
                    "--verify",
                    "HEAD^{commit}",
                ],
                env=env,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL,
                timeout=15,
            )
            .decode("ascii")
            .strip()
        )
    except (OSError, subprocess.SubprocessError, UnicodeError):
        raise ValueError("Prepared checkout unavailable") from None
    if actual != expected or not (checkout / ".git").is_dir():
        raise ValueError("Prepared checkout pin mismatch")


def prepare(config, api, docker):
    _revision(config.bolt_checkout, config.bolt_sha)
    _revision(config.driver_dir, config.driver_sha)
    if any(not (jdk / "bin/java").is_file() for jdk in config.jdk_dirs.values()):
        raise ValueError("Prepared JDK unavailable")
    if any(not seed.is_dir() for seed in config.cache_seed_dirs.values()):
        raise ValueError("Prepared cache unavailable")
    if config.arrow_source_dir and not config.arrow_source_dir.is_dir():
        raise ValueError("Prepared Arrow sources unavailable")
    if config.worker_tools_dir and not config.worker_tools_dir.is_dir():
        raise ValueError("Prepared worker tools unavailable")
    if not docker.verify_network():
        raise DockerError()
    api.check()


def run_service(service, seconds, once):
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    while not stopped.is_set():
        wait = seconds
        try:
            service.tick()
        except ApiError as error:
            logging.warning("GitHub operation deferred (%s)", error)
            wait = max(seconds, error.retry_after)
            if once:
                return 1
        except (DockerError, OSError, sqlite3.Error):
            logging.warning("Controller operation deferred")
            if once:
                return 1
        if once:
            return 0
        stopped.wait(wait)
    return 0


def start(args):
    c = Config.load(args.config)
    command = args.command
    docker = Docker(c) if command in {"runner", "prepare"} else None
    api = (
        GitHub(c.repository, c.read_token(optional=command == "prepare"))
        if command != "status"
        else None
    )
    if command == "prepare":
        prepare(c, api, docker)
        print("Prepared worker contract verified")
        return 0
    if command == "enqueue" and not positive(args.pr):
        raise ValueError("Invalid PR number")
    if command == "enqueue" and args.dry_run:
        # A preview must not create or modify the live queue.
        with tempfile.TemporaryDirectory(prefix="bolt-ci-preview-") as temp:
            state = State(Path(temp), c.repository, c.matrix, c.fingerprint)
            listener = Listener(state, api, c.maintainer_logins, c.only_pr)
            result = listener.enqueue(args.pr, True)
            print(json.dumps(result, sort_keys=True))
        return 0
    state = State(c.state_dir, c.repository, c.matrix, c.fingerprint)
    if command == "status":
        print(json.dumps(state.status(), sort_keys=True))
        return 0
    if command in {"listener", "enqueue"}:
        service = Listener(state, api, c.maintainer_logins, c.only_pr)
        if command == "enqueue":
            print(json.dumps(service.enqueue(args.pr, False), sort_keys=True))
            return 0
        return run_service(service, c.poll_seconds, args.once)
    if command == "runner":
        service = Runner(
            c,
            state,
            api,
            docker,
            Workspaces(c),
        )
        return run_service(service, c.runner_poll_seconds, args.once)
    writer_token = c.read_token(writer=True)
    if hmac.compare_digest(api.token, writer_token):
        raise ValueError("Reader and commenter must use distinct credentials")
    writer = GitHub(c.repository, writer_token)
    service = Commenter(state, api, writer, only_pr=c.only_pr)
    return run_service(service, c.commenter_poll_seconds, args.once)


def main():
    arguments = parser()
    args = arguments.parse_args()
    if not args.config:
        arguments.error("--config or BOLT_CI_CONFIG is required")
    os.umask(0o077)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        return start(args)
    except (ApiError, DockerError) as error:
        logging.error("%s", error)
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, sqlite3.Error):
        logging.error("Invalid deployment configuration or controller operation")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
