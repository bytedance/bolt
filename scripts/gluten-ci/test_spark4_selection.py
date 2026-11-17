# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Guard the source selection behind the full Spark 4 nightly cells."""

import os
from pathlib import Path
import re
import unittest
import xml.etree.ElementTree as ET

ROOT = Path(os.environ.get("GLUTEN_TEST_SOURCE", "/nonexistent-gluten-checkout"))
SHARED = ROOT / "gluten-ut/spark40/src/test/bolt-shared"


def settings(version, backend):
    source = "scala" if backend == "velox" else "backends-bolt"
    return ROOT / (
        f"gluten-ut/spark{version}/src/test/{source}/org/apache/"
        f"gluten/utils/{backend}/{backend.title()}TestSettings.scala"
    )


def declared_classes(version):
    classes = {}
    for directory in (
        ROOT / "gluten-ut/common/src/test/scala",
        ROOT / f"gluten-ut/spark{version}/src/test/scala",
        SHARED,
    ):
        for path in directory.rglob("*.scala"):
            text = path.read_text()
            package = re.search(r"^package ([\w.]+)", text, re.M)
            if package:
                for name in re.findall(r"^class (\w+)", text, re.M):
                    classes.setdefault(name, set()).add(package[1] + "." + name)
    return classes


def resolve(name, classes):
    if name.startswith("org."):
        return name
    if name.startswith("aggregate."):
        return "org.apache.spark.sql.catalyst.expressions." + name
    if name in ("GlutenProductAggSuite", "GlutenApproxTopKSuite"):
        return "org.apache.spark.sql." + name
    if name == "GlutenSQLMetricsSuite":
        return "org.apache.spark.sql.execution.metric." + name
    candidates = classes[name]
    assert len(candidates) == 1, (name, candidates)
    return next(iter(candidates))


@unittest.skipUnless(
    (ROOT / "gluten-ut").is_dir(),
    "set GLUTEN_TEST_SOURCE to test Apache suite selection",
)
class Spark4SelectionTest(unittest.TestCase):
    def test_every_velox_common_suite_is_selected_or_explicitly_excluded(self):
        # Catch removing any applicable suite, including version-only suites,
        # even when unrelated suites still produce a nonzero test report.
        shared = "\n".join(p.read_text() for p in SHARED.rglob("*.scala"))
        for version in ("40", "41", "42"):
            with self.subTest(version=version):
                classes = declared_classes(version)
                reference = settings(version, "velox").read_text()
                expected = {
                    resolve(n, classes)
                    for n in re.findall(r"^  enableSuite\[([^]]+)\]", reference, re.M)
                }
                path = settings(version, "bolt")
                actual = shared + (path.read_text() if path.exists() else "")
                enabled = set(re.findall(r"enableSuite\[\s*([\w.]+)\s*\]", actual))
                disabled = set(re.findall(r"disableSuite\[\s*([\w.]+)\s*\]", actual))
                disabled.update(re.findall(r'disableSuite\(\s*"([\w.]+)"', actual))
                self.assertEqual(expected - enabled - disabled, set())
                # Only these backend-specific differences may exclude Velox-enabled suites.
                self.assertEqual(
                    expected & disabled,
                    {
                        name
                        for name in expected
                        if "PlanStability" in name
                        or name.endswith(".VeloxAdaptiveQueryExecSuite")
                    },
                )
                available = set().union(*classes.values())
                self.assertFalse((enabled | disabled) - available)
                self.assertFalse(enabled & disabled)
                for name in (
                    "GlutenWholeStageCodegenSuite",
                    "GlutenLogicalPlanTagInSparkPlanSuite",
                    "GlutenDataFrameSuite",
                    "GlutenJoinSuite",
                    "GlutenDataFrameAggregateSuite",
                ):
                    self.assertIn(resolve(name, classes), enabled)

    def test_each_profile_compiles_shared_and_version_specific_selectors(self):
        ns = {"m": "http://maven.apache.org/POM/4.0.0"}
        for version in ("40", "41", "42"):
            with self.subTest(version=version):
                root = ET.parse(ROOT / f"gluten-ut/spark{version}/pom.xml")
                profile = next(
                    p
                    for p in root.findall(".//m:profile", ns)
                    if p.findtext("m:id", namespaces=ns) == "backends-bolt"
                )
                sources = [s.text for s in profile.findall(".//m:source", ns)]
                self.assertIn("src/test/backends-bolt", sources)
                self.assertTrue(
                    any(s.endswith("spark40/src/test/bolt-shared") for s in sources)
                )
