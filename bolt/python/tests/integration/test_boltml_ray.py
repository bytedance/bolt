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

"""Run installed BoltML through Ray Client and real workers on a local cluster."""

import hashlib
from importlib.metadata import version
import os
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pybolt
import pybolt.pybolt as native
import ray
from ray.cluster_utils import Cluster

from boltml import DataFrame
from boltml.distributed.ray import RayExecutionConfig, RayExecutionMode
from boltml.executor.ray import RayExecutor
from boltml.function import scalarFunction
from boltml.plan_builder.substrait import SubstraitPlanBuilderFactory


class RayExecutionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix="bolt-ray-")
        cls.addClassCleanup(cls.temporary.cleanup)
        environment = patch.dict(
            os.environ,
            {
                "RAY_TMPDIR": cls.temporary.name,
                "RAY_USAGE_STATS_ENABLED": "0",
                "RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER": "0",
            },
        )
        environment.start()
        cls.addClassCleanup(environment.stop)
        # Prevent Ray Client from replacing our explicit loopback GCS address.
        localNetwork = patch("ray._private.ray_constants.ENABLE_RAY_CLUSTER", False)
        localNetwork.start()
        cls.addClassCleanup(localNetwork.stop)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        cls.cluster = Cluster(shutdown_at_exit=True)
        cls.addClassCleanup(cls.cluster.shutdown)
        cls.cluster.add_node(
            node_ip_address="127.0.0.1",
            num_cpus=4,
            object_store_memory=128 * 1024 * 1024,
            include_dashboard=False,
            ray_client_server_port=port,
            temp_dir=str(Path(cls.temporary.name) / "ray"),
        )
        cls.addClassCleanup(ray.shutdown)
        ray.init(address=f"ray://127.0.0.1:{port}", namespace="bolt-integration-tests")

    def executor(self):
        return RayExecutor(config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF))

    def assertRemoteTasks(self, executor, expectedCount):
        summary = executor.lastExecutionSummary
        self.assertIsNotNone(summary)
        self.assertEqual(summary.executionMode, RayExecutionMode.REMOTE_LEAF)
        self.assertEqual(len(summary.stages), 1)
        stage = summary.stages[0]
        self.assertTrue(stage.remote)
        self.assertEqual(stage.remoteTaskCount, expectedCount)
        self.assertIsNone(stage.fallbackReason)
        self.assertTrue(stage.workerId or stage.workerIds)

    def testWorkerImportsAndExecutesNativeLibrary(self):
        def probe():
            import hashlib
            from importlib.metadata import version
            import os
            from pathlib import Path

            import pybolt
            import pybolt.pybolt as native

            source = pybolt.rowVector(["x"], [pybolt.fromList([2, 3])])
            plan = (
                pybolt.PlanBuilder()
                .values([source])
                .project(["x * CAST(2 AS BIGINT) AS result"])
            )
            result = pybolt.BoltTaskExecutor().execute(
                plan.planFragment(), "integration-ray-probe"
            )
            nativePath = Path(native.__file__)
            with nativePath.open("rb") as binary:
                digest = hashlib.file_digest(binary, "sha256").hexdigest()
            return {
                "pid": os.getpid(),
                "native": str(nativePath),
                "digest": digest,
                "version": version("bolt-engine"),
                "values": list(result.childAt("result")),
            }

        result = ray.get(ray.remote(probe).remote(), timeout=60)
        self.assertNotEqual(result["pid"], os.getpid())
        self.assertTrue(result["native"].endswith(".so"))
        self.assertEqual(result["version"], version("bolt-engine"))
        with Path(native.__file__).open("rb") as binary:
            self.assertEqual(
                result["digest"], hashlib.file_digest(binary, "sha256").hexdigest()
            )
        self.assertEqual(result["values"], [4, 6])

    def testRemoteDataframeProjection(self):
        executor = self.executor()
        frame = DataFrame({"x": [1, 2, 3]}, executor=executor)
        frame.transform(result=lambda df: df["x"] + 1)
        self.assertEqual(list(frame["result"]), [2, 3, 4])
        self.assertRemoteTasks(executor, 1)

    def testScalarUdfExecutesInRemoteWorker(self):
        executor = self.executor()

        @scalarFunction(pybolt.BigintType(), executors=executor)
        def integrationRemoteProcessId(value):
            import os

            return os.getpid() + value

        frame = DataFrame({"x": [0, 1]}, executor=executor)
        frame.transform(result=lambda df: df["x"].map(integrationRemoteProcessId))
        values = list(frame["result"])
        self.assertGreater(values[0], 0)
        self.assertNotEqual(values[0], os.getpid())
        self.assertEqual(values[1], values[0] + 1)
        self.assertEqual(len(executor.udfRegistry), 1)
        self.assertRemoteTasks(executor, 1)

    def testValuesInputDoesNotPretendToFanOut(self):
        with patch.dict(os.environ, {"BOLTML_LEAF_PARALLELISM": "4"}):
            executor = self.executor()
            frame = DataFrame({"x": [1, 2, 3]}, executor=executor)
            frame.transform(result=lambda df: df["x"] * 2)
            self.assertEqual(list(frame["result"]), [2, 4, 6])
            self.assertRemoteTasks(executor, 1)

    def testFourFileSplitsRunAsFourRemoteTasks(self):
        with tempfile.TemporaryDirectory(prefix="bolt-integration-splits-") as folder:
            paths = []
            for index in range(4):
                path = Path(folder) / f"part-{index}.parquet"
                pq.write_table(pa.table({"x": [index * 2, index * 2 + 1]}), path)
                paths.append(path.as_uri())
            schema = pybolt.RowType(["x"], [pybolt.BigintType()])
            factory = SubstraitPlanBuilderFactory()
            with patch.dict(os.environ, {"BOLTML_LEAF_PARALLELISM": "4"}):
                executor = self.executor()
                frame = DataFrame(
                    factory.fromLocalFiles(paths, schema, pybolt.FileFormat.PARQUET),
                    planFactory=factory,
                    executor=executor,
                )
                frame.filter(frame["x"] >= 2).select(["x"])
                self.assertEqual(sorted(frame["x"]), [2, 3, 4, 5, 6, 7])
                self.assertRemoteTasks(executor, 4)


if __name__ == "__main__":
    unittest.main(verbosity=2)
