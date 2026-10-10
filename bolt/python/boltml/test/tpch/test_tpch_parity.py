# Copyright (c) ByteDance Ltd. and/or its affiliates
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

import unittest

from .tpch_harness import PHASED_TPCH_QUERY_SUBSETS
from .tpch_harness import TpchParityHarness


class TestTpchParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.harness = TpchParityHarness()

    def testPhase1SubsetParityLocalVsStagedFile(self):
        for spec in PHASED_TPCH_QUERY_SUBSETS["phase1"]:
            with self.subTest(query=spec.name):
                self.harness.assertFileParity(self, spec)

    def testPhase1SubsetParityLocalVsRemoteLeaf(self):
        for spec in PHASED_TPCH_QUERY_SUBSETS["phase1"]:
            with self.subTest(query=spec.name):
                self.harness.assertRemoteLeafParity(self, spec)

    def testPhase2SubsetParityLocalVsStagedFile(self):
        for spec in PHASED_TPCH_QUERY_SUBSETS["phase2"]:
            with self.subTest(query=spec.name):
                self.harness.assertFileParity(self, spec)

    def testPhase3SubsetParityLocalVsStagedFile(self):
        for spec in PHASED_TPCH_QUERY_SUBSETS["phase3"]:
            with self.subTest(query=spec.name):
                self.harness.assertFileParity(self, spec)

    def testPhase3SubsetParityLocalVsStagedObject(self):
        for spec in PHASED_TPCH_QUERY_SUBSETS["phase3"]:
            with self.subTest(query=spec.name):
                self.harness.assertObjectParity(self, spec)

    def testPhase3SubsetParityLocalVsRemoteLeaf(self):
        for spec in PHASED_TPCH_QUERY_SUBSETS["phase3"]:
            with self.subTest(query=spec.name):
                self.harness.assertRemoteLeafParity(self, spec)

    def testPhase4SubsetParityLocalVsStagedFile(self):
        for spec in PHASED_TPCH_QUERY_SUBSETS["phase4"]:
            with self.subTest(query=spec.name):
                self.harness.assertFileParity(self, spec)

    def testPhase4SubsetParityLocalVsStagedObject(self):
        for spec in PHASED_TPCH_QUERY_SUBSETS["phase4"]:
            with self.subTest(query=spec.name):
                self.harness.assertObjectParity(self, spec)

    def testPhase4SubsetParityLocalVsRemoteLeaf(self):
        for spec in PHASED_TPCH_QUERY_SUBSETS["phase4"]:
            with self.subTest(query=spec.name):
                self.harness.assertRemoteLeafParity(self, spec)


if __name__ == "__main__":
    unittest.main()
