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

import os
import time
from pathlib import Path
import sys
from uuid import uuid4

import ray


def ensureRayInitialized():
    if not ray.is_initialized():
        envVars = {
            "PYTHONPATH": os.pathsep.join(path for path in sys.path if path),
        }
        for key in ("LD_PRELOAD", "LD_LIBRARY_PATH"):
            if key in os.environ:
                envVars[key] = os.environ[key]
        ray.init(
            ignore_reinit_error=True,
            include_dashboard=False,
            logging_level="ERROR",
            namespace="boltml",
            runtime_env={
                "py_modules": [str(Path(__file__).resolve().parents[1])],
                "env_vars": envVars,
            },
        )


@ray.remote
class _ExchangeRegistryActor:
    def __init__(self):
        self._descriptors = {}
        self._objects = {}
        self._pendingExchanges = {}
        # Per-key creation timestamps (monotonic seconds since the actor
        # process started). Used by ``sweep()`` to age out registrations
        # whose driver died before the finally-path cleanup ran — without
        # this the detached actor would otherwise pin object-store
        # entries and accumulate stale descriptor state indefinitely.
        self._descriptorTimestamps = {}
        self._objectTimestamps = {}
        self._pendingTimestamps = {}

    @staticmethod
    def _now() -> float:
        return time.monotonic()

    def _stamp(self, kind: str, key) -> None:
        """Record a creation/refresh timestamp for *key* in the matching
        per-kind dict. Called from every mutation that creates or
        replaces a registry entry. The actor itself has no clock
        besides its own ``time.monotonic``, which is fine for relative
        TTL math (driver and actor live in different processes, but
        the actor's clock is the only one consulted)."""
        bucket = {
            "descriptor": self._descriptorTimestamps,
            "object": self._objectTimestamps,
            "pending": self._pendingTimestamps,
        }[kind]
        bucket[key] = self._now()

    def sweep(self, maxAgeSec: float) -> dict:
        """Drop registry entries older than *maxAgeSec*.

        Called by ``getExchangeRegistry`` on every driver-side
        attachment so a fresh driver picks up the actor and immediately
        evicts orphans from previously-crashed drivers. The driver's
        finally-path ``deleteExchange`` / ``deleteObject`` calls remain
        the primary cleanup mechanism; this sweep is the safety net
        for the case where the driver never reached its finally block
        (segfault, SIGKILL, OOM-kill).

        Liveness-aware: descriptors and objects are linked by
        ``objectKey`` (set on every object-transport descriptor at
        publish). Before evicting old objects, refresh the timestamps
        of any object that's still referenced by a live (non-evicted)
        descriptor — otherwise a long-running exchange whose
        descriptors get refreshed by new lookups would eventually
        outlive its own payload, leaving consumers to resolve a
        descriptor whose ``objectKey`` no longer exists in the registry.
        """
        if maxAgeSec <= 0:
            return {"descriptors": 0, "objects": 0, "pending": 0}
        cutoff = self._now() - maxAgeSec

        # Bucket 1: descriptors. Evict first so we know which ones
        # survive into the object-liveness refresh.
        stale_descriptor_keys = [
            k for k, ts in self._descriptorTimestamps.items() if ts < cutoff
        ]
        for k in stale_descriptor_keys:
            self._descriptors.pop(k, None)
            self._descriptorTimestamps.pop(k, None)

        # Refresh object timestamps for any objectKey still referenced
        # by a surviving descriptor. ``getOwnedValue`` style: we don't
        # know the schema of ``descriptors`` (which is just the user's
        # tuple of descriptor records), so duck-type on ``objectKey``.
        now = self._now()
        for _ownerId, descriptors in self._descriptors.values():
            for descriptor in descriptors:
                objectKey = getattr(descriptor, "objectKey", None)
                if objectKey is not None and objectKey in self._objects:
                    self._objectTimestamps[objectKey] = now

        # Bucket 2: objects. Sweep after the refresh.
        stale_object_keys = [
            k for k, ts in self._objectTimestamps.items() if ts < cutoff
        ]
        for k in stale_object_keys:
            self._objects.pop(k, None)
            self._objectTimestamps.pop(k, None)

        # Bucket 3: pending exchanges — independent of descriptors /
        # objects (they're the *uncommitted* pre-state of a not-yet-
        # registered exchange). Plain TTL eviction.
        stale_pending_keys = [
            k for k, ts in self._pendingTimestamps.items() if ts < cutoff
        ]
        for k in stale_pending_keys:
            self._pendingExchanges.pop(k, None)
            self._pendingTimestamps.pop(k, None)

        return {
            "descriptors": len(stale_descriptor_keys),
            "objects": len(stale_object_keys),
            "pending": len(stale_pending_keys),
        }

    @staticmethod
    def _attempt(descriptors) -> int:
        if not descriptors:
            return 0
        attempts = {descriptor.attemptId for descriptor in descriptors}
        if len(attempts) != 1:
            raise ValueError(f"Inconsistent descriptor attempts: {sorted(attempts)}")
        return next(iter(attempts))

    def registerExchange(
        self, ownerId: str, executionId: str, stageId: str, exchangeId: str, descriptors
    ):
        key = (executionId, stageId, exchangeId)
        existing = self._descriptors.get(key)
        if existing is not None:
            existingOwnerId, existingDescriptors = existing
            if ownerId != existingOwnerId:
                return {"accepted": False, "descriptors": existingDescriptors}
            if self._attempt(descriptors) < self._attempt(existingDescriptors):
                return {"accepted": False, "descriptors": existingDescriptors}
        self._descriptors[key] = (ownerId, descriptors)
        self._stamp("descriptor", key)
        return {"accepted": True, "descriptors": descriptors}

    def prepareExchange(
        self,
        ownerId: str,
        executionId: str,
        stageId: str,
        exchangeId: str,
        attemptId: int,
    ):
        key = (executionId, stageId, exchangeId)
        existing = self._descriptors.get(key)
        if existing is not None:
            existingOwnerId, existingDescriptors = existing
            if ownerId != existingOwnerId:
                return {"accepted": False, "descriptors": existingDescriptors}
            if attemptId < self._attempt(existingDescriptors):
                return {"accepted": False, "descriptors": existingDescriptors}

        pending = self._pendingExchanges.get(key)
        if pending is not None:
            pendingOwnerId, pendingToken, pendingAttemptId = pending
            if ownerId != pendingOwnerId:
                return {"accepted": False, "descriptors": ()}
            if attemptId < pendingAttemptId:
                return {"accepted": False, "descriptors": ()}

        token = uuid4().hex
        self._pendingExchanges[key] = (ownerId, token, attemptId)
        self._stamp("pending", key)
        return {"accepted": True, "token": token}

    def commitExchange(
        self,
        ownerId: str,
        executionId: str,
        stageId: str,
        exchangeId: str,
        token: str,
        descriptors,
    ):
        key = (executionId, stageId, exchangeId)
        pending = self._pendingExchanges.get(key)
        if pending is None:
            return {"accepted": False, "descriptors": ()}
        pendingOwnerId, pendingToken, pendingAttemptId = pending
        if ownerId != pendingOwnerId or token != pendingToken:
            return {"accepted": False, "descriptors": ()}
        if self._attempt(descriptors) != pendingAttemptId:
            return {"accepted": False, "descriptors": ()}
        self._pendingExchanges.pop(key, None)
        self._pendingTimestamps.pop(key, None)
        self._descriptors[key] = (ownerId, descriptors)
        self._stamp("descriptor", key)
        return {"accepted": True, "descriptors": descriptors}

    def abortExchange(
        self, ownerId: str, executionId: str, stageId: str, exchangeId: str, token: str
    ):
        key = (executionId, stageId, exchangeId)
        pending = self._pendingExchanges.get(key)
        if pending is None:
            return False
        pendingOwnerId, pendingToken, _ = pending
        if ownerId != pendingOwnerId or token != pendingToken:
            return False
        self._pendingExchanges.pop(key, None)
        self._pendingTimestamps.pop(key, None)
        return True

    def resolve(self, executionId: str, stageId: str, exchangeId: str):
        key = (executionId, stageId, exchangeId)
        entry = self._descriptors.get(key)
        if entry is None:
            return None
        # Refresh the descriptor's liveness on every successful resolve
        # so an actively-used long-running exchange doesn't get evicted
        # by a TTL sweep just because its initial publish was older
        # than the TTL. The sweep is a safety net for orphaned state,
        # not a hard cap on exchange lifetime.
        self._stamp("descriptor", key)
        return entry[1]

    def deleteExchange(
        self, ownerId: str, executionId: str, stageId: str, exchangeId: str
    ):
        key = (executionId, stageId, exchangeId)
        self._pendingExchanges.pop(key, None)
        self._pendingTimestamps.pop(key, None)
        entry = self._descriptors.get(key)
        if entry is None:
            return False
        if entry[0] != ownerId:
            return False
        self._descriptors.pop(key, None)
        self._descriptorTimestamps.pop(key, None)
        return True

    def registerObject(self, ownerId: str, objectKey: str, objectRef):
        entry = self._objects.get(objectKey)
        if entry is not None and entry[0] != ownerId:
            return False
        self._objects[objectKey] = (ownerId, objectRef)
        self._stamp("object", objectKey)
        return True

    def getObject(self, objectKey: str):
        entry = self._objects.get(objectKey)
        if entry is None:
            return None
        # Same liveness refresh as ``resolve`` — an in-use object
        # shouldn't be evicted just because its registration timestamp
        # crossed the TTL.
        self._stamp("object", objectKey)
        return entry[1]

    def deleteObject(self, ownerId: str, objectKey: str):
        entry = self._objects.get(objectKey)
        if entry is None:
            return False
        if entry[0] != ownerId:
            return False
        self._objects.pop(objectKey, None)
        self._objectTimestamps.pop(objectKey, None)
        return True


# Process-local cache for the registry actor handle. Ray's
# ``ActorHandle._actor_ref`` consults a thread-local registry
# (``_ray_local_actor_handles``) that's populated when an actor handle
# is **created** via ``ActorClass.options(...).remote()`` — i.e. the
# "creator" handle. Subsequent ``ray.get_actor(name, namespace=...)``
# calls return a different handle object whose actor id isn't always
# resolvable through the local registry, and invoking ``.remote(...)``
# on it then raises ``RuntimeError: Lost reference to actor``.
#
# The bug surfaces specifically when a caller mixes the cached
# creator-handle (e.g. ``AdaptiveExchangeManager.__registry``) with a
# fresh handle obtained via the standalone ``publishExchangePartition``
# helper (which used to call ``getExchangeRegistry()`` again at every
# entry). The cached one keeps working; the fresh one fails. Holding a
# single process-long-lived strong reference avoids the issue entirely
# and matches Ray's documented "don't re-acquire detached actor handles
# via ``get_actor`` from the creating process" guidance.
_REGISTRY_HANDLE = None


def getExchangeRegistry():
    """Return the single process-wide handle to the boltml exchange
    registry actor. Idempotent: the first call creates (or attaches to)
    the detached actor and caches its handle; subsequent calls return
    the cached handle so every caller shares the registration that Ray
    tracks reliably.

    Concurrency note: this is intentionally not thread-locked. The
    boltml driver-side code paths that reach this function are
    sequential (DataFrame execution, exchange publish/load); the
    detached actor itself serialises actor-method invocations Ray-side.
    If a future caller needs thread-safe lazy initialisation, wrap the
    body in a ``threading.Lock`` — there is no shared-mutable-state
    invariant at risk beyond the cache slot itself.
    """
    global _REGISTRY_HANDLE
    ensureRayInitialized()
    if _REGISTRY_HANDLE is not None:
        return _REGISTRY_HANDLE
    try:
        _REGISTRY_HANDLE = ray.get_actor("boltml_exchange_registry", namespace="boltml")
        # We just attached to an existing detached actor from a previous
        # driver. If that driver crashed before its finally-path
        # cleanup, the actor still holds object refs and descriptors —
        # which pin Ray object-store memory indefinitely. Run an
        # opportunistic age-based sweep so the new driver starts clean.
        # TTL defaults to 6h, which is well past any reasonable
        # execution wall time on the workloads this engine targets;
        # operators with shorter runtimes can shrink it via
        # ``BOLTML_REGISTRY_SWEEP_TTL_SEC``.
        try:
            ttl = float(os.environ.get("BOLTML_REGISTRY_SWEEP_TTL_SEC", "21600"))
        except ValueError:
            ttl = 21600.0
        try:
            ray.get(_REGISTRY_HANDLE.sweep.remote(ttl))
        except Exception:  # noqa: BLE001
            # Sweep is best-effort; don't fail registry attach.
            pass
    except ValueError:
        _REGISTRY_HANDLE = _ExchangeRegistryActor.options(
            name="boltml_exchange_registry",
            namespace="boltml",
            lifetime="detached",
        ).remote()
    return _REGISTRY_HANDLE
