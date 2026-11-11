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

"""Deployment validation across the host and Docker boundaries."""

import importlib.util
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]


class DeploymentTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.settings = {
            "HOST_DATA_DIR": str(self.root),
            "CI_IMAGE": "localhost:5000/bolt-ci:20260921",
            "BOLT_REF": "a" * 40,
            "READ_TOKEN_FILE": str(self.root / "reader"),
            "BOT_TOKEN_FILE": str(self.root / "writer"),
            "ONLY_PR": "32",
        }

    def module(self, name):
        spec = importlib.util.find_spec(name)
        self.assertIsNotNone(spec, f"Missing deployment module {name}")
        return importlib.import_module(name)

    def test_environment_rejects_invalid_inputs_before_docker(self):
        prepare = self.module("prepare")
        for key, invalid in (
            ("HOST_DATA_DIR", ""),
            ("HOST_DATA_DIR", "relative/path"),
            ("HOST_DATA_DIR", str(self.root) + "/.."),
            ("CI_IMAGE", "-unsafe"),
            ("CI_IMAGE", "localhost:5000/bolt-ci"),
            ("BOLT_REF", "main"),
            ("ONLY_PR", "0"),
            ("DOCKER_NETWORK", "host"),
            ("CONAN_REMOTE", "http://user:password@127.0.0.1:9300"),
            ("CONAN_REMOTE", "http://100.100.100.200:80"),
            ("CONAN_REMOTE", "http://169.254.169.254:80"),
        ):
            with self.subTest(key=key, invalid=invalid), self.assertRaises(ValueError):
                prepare.settings(self.settings | {key: invalid})
        self.assertEqual(prepare.settings(self.settings)["only_pr"], 32)

    def test_firewall_cannot_renew_after_owned_hook_is_shadowed(self):
        guard = self.module("network_guard")
        exact = (
            "-A INPUT -i bglut-worker -j BGLUT-W-IN",
            "-A BGLUT-W-IN -j DROP",
        )
        clean = "-P INPUT ACCEPT\n-N BGLUT-W-IN\n" + "\n".join(exact) + "\n"
        guard.verify_chain(clean, "INPUT", "BGLUT-W-IN", exact)
        shadowed = clean.replace(exact[0], "-A INPUT -j ACCEPT\n" + exact[0])
        with self.assertRaises(ValueError):
            guard.verify_chain(shadowed, "INPUT", "BGLUT-W-IN", exact)

    def test_firewall_allowance_is_exact_and_includes_dnat_identity(self):
        guard = self.module("network_guard")
        _, forwarded = guard.rules("eth0", ("172.17.0.1", 9300), ("172.20.0.2", 9300))
        permits = [line for line in forwarded if line.endswith(" -j ACCEPT")]
        self.assertEqual(len(permits), 1)
        self.assertIn("-d 172.20.0.2/32", permits[0])
        self.assertIn("--dport 9300", permits[0])
        self.assertIn("--ctorigdst 172.17.0.1 --ctorigdstport 9300", permits[0])
        self.assertTrue(any("-d 100.64.0.0/10 -j DROP" in line for line in forwarded))
        self.assertTrue(any("-d 169.254.0.0/16 -j DROP" in line for line in forwarded))

    def test_compose_uses_only_intended_role_mounts(self):
        compose = shutil.which("docker-compose")
        if not compose:
            self.skipTest("standalone Compose is unavailable")
        prepare = self.module("prepare")
        env = prepare.compose_env(self.root / "private")
        result = subprocess.run(
            [
                compose,
                "--env-file",
                "/dev/null",
                "-p",
                "bolt-gluten-ci",
                "--profile",
                "operator",
                "-f",
                str(HERE / "compose.yml"),
                "config",
                "--format",
                "json",
            ],
            env=os.environ | env,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        services = json.loads(result.stdout)["services"]
        reader = "/run/bolt-gluten-ci/reader"
        writer = "/run/bolt-gluten-ci/writer"
        socket = "/var/run/docker.sock"
        mounts = {
            role: {v["target"]: v for v in service["volumes"]}
            for role, service in services.items()
        }
        self.assertEqual(
            {role for role in mounts if writer in mounts[role]}, {"commenter"}
        )
        self.assertEqual(
            {role for role in mounts if reader in mounts[role]},
            {"listener", "runner", "commenter", "preview", "preflight"},
        )
        self.assertEqual(
            {role for role in mounts if socket in mounts[role]}, {"runner", "preflight"}
        )
        for role in (
            "listener",
            "runner",
            "commenter",
            "preview",
            "preflight",
            "status",
        ):
            self.assertEqual(list(services[role]["networks"]), ["control"])
        self.assertNotIn(env["CI_STATE"], mounts["preview"])
        self.assertTrue(mounts["runner"][env["CI_RECEIPTS"]]["read_only"])

    def test_compose_invocation_drops_inherited_project_overrides(self):
        prepare = self.module("prepare")
        with mock.patch.dict(
            os.environ, {"COMPOSE_FILE": "/other", "DOCKER_HOST": "tcp://bad"}
        ):
            command, env = prepare.compose_command(self.root / "private", ("ps",))
        self.assertIn("bolt-gluten-ci", command)
        self.assertIn(str(HERE / "compose.yml"), command)
        self.assertNotIn("COMPOSE_FILE", env)
        self.assertEqual(env["DOCKER_HOST"], "unix:///var/run/docker.sock")

    def test_bundle_rejects_changed_or_extra_files(self):
        bundle = self.module("bundle")
        (self.root / "file").write_text("trusted")
        bundle.write_manifest(self.root, "a" * 40, "a" * 40, "c" * 40)
        self.assertEqual(bundle.verify_manifest(self.root)["bolt_commit"], "a" * 40)
        (self.root / "extra").write_text("unexpected")
        with self.assertRaises(ValueError):
            bundle.verify_manifest(self.root)
        (self.root / "extra").unlink()
        (self.root / "file").write_text("replaced")
        with self.assertRaises(ValueError):
            bundle.verify_manifest(self.root)

    def test_bundle_requires_helpers_from_the_same_bolt_commit(self):
        bundle = self.module("bundle")
        (self.root / "file").write_text("trusted")
        bundle.write_manifest(self.root, "a" * 40, "b" * 40, "c" * 40)
        with self.assertRaises(ValueError):
            bundle.verify_manifest(self.root)

    def test_legacy_gluten_helper_bundle_is_rejected(self):
        bundle = self.module("bundle")
        bundle.write_manifest(self.root, "a" * 40, "a" * 40, "c" * 40)
        path = self.root / "MANIFEST.json"
        manifest = json.loads(path.read_text())
        manifest["schema"] = 1
        manifest["gluten_commit"] = manifest.pop("helper_commit")
        path.write_text(json.dumps(manifest))
        with self.assertRaises(ValueError):
            bundle.verify_manifest(self.root)

    def test_guard_removes_previous_receipt_when_kernel_verification_fails(self):
        guard = self.module("network_guard")
        receipt = self.root / "receipt.json"
        boot_id = self.root / "boot-id"
        stamp = self.root / "boot-stamp"
        boot_id.write_text("new-boot")
        stamp.write_text("new-boot")
        plan = {
            "network": "bolt-gluten-ci-workers",
            "network_id": "1" * 64,
            "image": "localhost:5000/bolt-ci@sha256:" + "2" * 64,
        }
        with mock.patch.object(guard, "BOOT_ID", boot_id):
            with mock.patch.object(guard, "enforce", return_value=None):
                guard.renew(plan, receipt, stamp)
            self.assertEqual(json.loads(receipt.read_bytes())["boot_id"], "new-boot")
            with mock.patch.object(
                guard, "enforce", side_effect=ValueError("hook lost")
            ):
                with self.assertRaises(ValueError):
                    guard.renew(plan, receipt, stamp)
            self.assertFalse(receipt.exists())

    def test_jdk_download_rejects_a_same_size_archive_with_wrong_checksum(self):
        from io import BytesIO

        prepare = self.module("prepare")
        archive = self.root / "archive.tar.gz"
        response = BytesIO(b"modified JDK archive")
        response.geturl = lambda: "https://release-assets.githubusercontent.com/archive"
        with mock.patch.object(
            prepare.urllib.request, "urlopen", return_value=response
        ):
            with self.assertRaisesRegex(ValueError, "checksum"):
                prepare.download_jdk_archive(
                    "https://example.test", "0" * 64, 20, archive
                )

    def test_native_nftables_backend_is_rejected_even_with_old_iptables_hooks(self):
        guard = self.module("network_guard")
        with mock.patch.object(
            guard,
            "docker",
            return_value=b'{"ServerVersion":"29.0.0","FirewallBackend":{"Driver":"nftables"}}',
        ):
            with self.assertRaisesRegex(ValueError, "iptables"):
                guard.verify_backend()

    def test_stop_freezes_scheduling_and_stops_only_owned_live_workers(self):
        from types import SimpleNamespace

        prepare = self.module("prepare")
        operations = []
        active = (
            {"container": "owned-live"},
            {"container": "owned-exited"},
            {"container": "unowned"},
        )
        workers = {
            "owned-live": "running",
            "owned-exited": "exited",
            "owned-orphan": "running",
        }
        config = SimpleNamespace(
            state_dir=self.root,
            repository="apache/gluten",
            matrix=[],
            fingerprint="bundle",
        )
        state = SimpleNamespace(
            instance="this-instance",
            active=lambda: active,
            mark_stopping=lambda task, reason: operations.append(
                (task["container"], reason)
            ),
        )
        engine = SimpleNamespace(
            names=lambda instance: set(workers)
            if instance == "this-instance"
            else {"unowned"},
            inspect=lambda name: SimpleNamespace(status=workers[name]),
            stop=lambda name: operations.append(("docker", name)),
        )
        with (
            mock.patch.object(prepare.Config, "load", return_value=config),
            mock.patch.object(prepare, "State", return_value=state),
            mock.patch.object(prepare, "Docker", return_value=engine),
            mock.patch.object(
                prepare, "compose", side_effect=lambda _, *cmd: operations.append(cmd)
            ),
        ):
            prepare.stop(self.root)
        self.assertEqual(operations[0], ("stop", "listener", "runner"))
        self.assertEqual(operations[-1], ("stop", "commenter"))
        self.assertCountEqual(
            operations[1:-1],
            [
                ("owned-live", "interrupted"),
                ("docker", "owned-live"),
                ("docker", "owned-orphan"),
            ],
        )

    def test_bundle_exports_allowlisted_files_even_if_git_archive_ignores_them(self):
        bundle = self.module("bundle")
        source, out = self.root / "git-source", self.root / "out"
        source.mkdir()
        out.mkdir()
        subprocess.run(["git", "init", "-q", str(source)], check=True)
        (source / ".github").mkdir()
        (source / ".gitattributes").write_text(".github export-ignore\n")
        file = source / ".github/ci.sh"
        file.write_text("#!/bin/sh\nexit 0\n")
        file.chmod(0o755)
        subprocess.run(["git", "-C", str(source), "add", "."], check=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(source),
                "-c",
                "user.name=CI Fixture",
                "-c",
                "user.email=fixture@example.test",
                "-c",
                "core.hooksPath=/dev/null",
                "commit",
                "-qm",
                "test fixture",
            ],
            check=True,
        )
        commit = subprocess.check_output(
            ["git", "-C", str(source), "rev-parse", "HEAD"], text=True
        ).strip()
        bundle.export(source, commit, out, (".github/ci.sh",))
        exported = out / ".github/ci.sh"
        self.assertEqual(exported.read_text(), file.read_text())
        self.assertTrue(exported.stat().st_mode & 0o100)
        self.assertFalse((out / ".gitattributes").exists())
