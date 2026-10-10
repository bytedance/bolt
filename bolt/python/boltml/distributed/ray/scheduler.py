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

import ray

from ...logging import boltmlDebugLog
from .exchange_registry import ensureRayInitialized
from .remote_worker import executeRemoteLeafTask
from .remote_worker import executeRemoteMultiInputConsumer
from .remote_worker import RemoteExecutionActor
from .remote_worker import executeRemoteSingleInputConsumer


class RayLeafScheduler:
    def __init__(self, workerMode="task"):
        self._workerMode = workerMode
        self._actor = None

    def ensureInitialized(self):
        ensureRayInitialized()

    def _ensureActor(self):
        self.ensureInitialized()
        if self._actor is None:
            boltmlDebugLog("scheduler", "creating remote execution actor")
            self._actor = RemoteExecutionActor.remote()
        return self._actor

    def _useActorMode(self) -> bool:
        return getattr(self._workerMode, "value", self._workerMode) == "actor"

    def execute(self, taskSpec, outputPlaceholders=(), publishConfig=None):
        return ray.get(self.submit(taskSpec, outputPlaceholders, publishConfig))

    def submit(self, taskSpec, outputPlaceholders=(), publishConfig=None):
        if self._useActorMode():
            boltmlDebugLog(
                "scheduler", f"dispatch leaf task via actor stage={taskSpec.stageId}"
            )
            return self._ensureActor().executeLeafTask.remote(
                taskSpec, outputPlaceholders, publishConfig
            )
        self.ensureInitialized()
        boltmlDebugLog(
            "scheduler", f"dispatch leaf task via task stage={taskSpec.stageId}"
        )
        return executeRemoteLeafTask.remote(taskSpec, outputPlaceholders, publishConfig)

    def executeSingleInputConsumer(
        self, taskSpec, inputDescriptors, outputPlaceholders=(), publishConfig=None
    ):
        return ray.get(
            self.submitSingleInputConsumer(
                taskSpec, inputDescriptors, outputPlaceholders, publishConfig
            )
        )

    def submitSingleInputConsumer(
        self,
        taskSpec,
        inputDescriptors,
        outputPlaceholders=(),
        publishConfig=None,
        forceTaskMode=False,
    ):
        if self._useActorMode() and not forceTaskMode:
            boltmlDebugLog(
                "scheduler",
                f"dispatch single-input consumer via actor stage={taskSpec.stageId}",
            )
            return self._ensureActor().executeSingleInputConsumer.remote(
                taskSpec, inputDescriptors, outputPlaceholders, publishConfig
            )
        self.ensureInitialized()
        boltmlDebugLog(
            "scheduler",
            f"dispatch single-input consumer via task stage={taskSpec.stageId}",
        )
        return executeRemoteSingleInputConsumer.remote(
            taskSpec, inputDescriptors, outputPlaceholders, publishConfig
        )

    def executeMultiInputConsumer(
        self,
        taskSpec,
        lhsDescriptors,
        rhsDescriptors,
        outputPlaceholders=(),
        publishConfig=None,
    ):
        return ray.get(
            self.submitMultiInputConsumer(
                taskSpec,
                lhsDescriptors,
                rhsDescriptors,
                outputPlaceholders,
                publishConfig,
            )
        )

    def submitMultiInputConsumer(
        self,
        taskSpec,
        lhsDescriptors,
        rhsDescriptors,
        outputPlaceholders=(),
        publishConfig=None,
        forceTaskMode=False,
    ):
        if self._useActorMode() and not forceTaskMode:
            boltmlDebugLog(
                "scheduler",
                f"dispatch multi-input consumer via actor stage={taskSpec.stageId}",
            )
            return self._ensureActor().executeMultiInputConsumer.remote(
                taskSpec,
                lhsDescriptors,
                rhsDescriptors,
                outputPlaceholders,
                publishConfig,
            )
        self.ensureInitialized()
        boltmlDebugLog(
            "scheduler",
            f"dispatch multi-input consumer via task stage={taskSpec.stageId}",
        )
        return executeRemoteMultiInputConsumer.remote(
            taskSpec, lhsDescriptors, rhsDescriptors, outputPlaceholders, publishConfig
        )

    def wait(self, refs):
        self.ensureInitialized()
        return ray.get(list(refs))

    def waitWithPartialResults(self, refs, drainGraceSeconds: float = 10.0):
        """Like ``wait`` but on any failure also collects results from
        siblings that completed successfully and drains/cancels any
        still-running siblings before returning.

        Returns ``(results, error)`` where:
        * If everything succeeded, ``results`` is the full tuple in
          submission order and ``error`` is None — same as ``wait``.
        * If any task raised, ``results`` is a tuple of the same
          length as ``refs`` where successful slots hold their result
          and failed slots hold ``None``. ``error`` is the first
          exception raised. Callers can use the successful slots to
          clean up published artifacts (file paths, object keys)
          before re-raising the error.

        Drain protocol on failure:

        1. ``ray.wait(refs, timeout=drainGraceSeconds)`` to give all
           in-flight siblings a chance to reach a terminal state
           (success OR failure) before we move to cancellation. This
           catches the common case where a sibling was *about to*
           publish — bounded grace period turns the race into a
           wait that completes naturally.
        2. For any sibling still running after the grace period,
           call ``ray.cancel(ref, force=True)`` to terminate the
           worker process. Without ``force=True``, ``ray.cancel``
           only delivers an interrupt that running C++ code (e.g.
           ``executor.execute``) won't observe; the worker keeps
           running and may publish artifacts the driver doesn't
           know about.
        3. Collect whichever results are now available via
           ``ray.get(r, timeout=0)``. Cancelled / still-running
           refs become ``None``.

        Combined with descriptor-scoped cleanup at the call site,
        this gets remote publish to *effectively* stage-atomic
        under transient failures.
        """
        self.ensureInitialized()
        ref_list = list(refs)
        try:
            results = ray.get(ref_list)
            return tuple(results), None
        except Exception as error:  # noqa: BLE001
            # Failure path. Drain siblings with a bounded grace
            # period, then force-cancel anything still running.
            try:
                _ready, not_ready = ray.wait(
                    ref_list,
                    num_returns=len(ref_list),
                    timeout=drainGraceSeconds,
                )
            except Exception:  # noqa: BLE001
                not_ready = []
            for r in not_ready:
                try:
                    ray.cancel(r, force=True)
                except Exception:  # noqa: BLE001
                    pass
            # Now collect whatever's available.
            partial: list = []
            for r in ref_list:
                try:
                    partial.append(ray.get(r, timeout=0))
                except Exception:  # noqa: BLE001
                    partial.append(None)
            return tuple(partial), error
