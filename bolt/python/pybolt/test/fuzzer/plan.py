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


def _plan_node_names(plan):
    plan_text = plan.planNode().toString(recursive=True, withId=False)
    names = []
    for line in plan_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("-- "):
            names.append(stripped[3:].split("[", 1)[0])
    return names


def _assert_plan_node_names(test_case, plan, expected_names):
    test_case.assertEqual(_plan_node_names(plan), expected_names)
