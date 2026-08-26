#!/usr/bin/env python3
"""Reference implementation for the persistent scheduler contract."""

from __future__ import annotations

import copy
import fcntl
import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


class Scheduler:
    """A small file-backed scheduler with serialized state transitions."""

    def __init__(self, state_path: str | os.PathLike[str]) -> None:
        self.path = Path(state_path)
        self.lock_path = Path(str(self.path) + ".lock")

    @classmethod
    def create(cls, state_path: str | os.PathLike[str]) -> "Scheduler":
        scheduler = cls(state_path)
        scheduler.path.parent.mkdir(parents=True, exist_ok=True)
        with scheduler._lock_fd():
            scheduler._write_state(scheduler._empty_state())
        return scheduler

    @staticmethod
    def _empty_state() -> dict[str, Any]:
        return {"version": 1, "jobs": {}, "occurrences": {}}

    @staticmethod
    def _int(value: Any, name: str, positive: bool = False) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{name} must be an integer")
        if positive and value <= 0:
            raise ValueError(f"{name} must be positive")
        return value

    @contextmanager
    def _lock_fd(self) -> Iterator[None]:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            # BUG: this lock is process-local in practice because the shared flock
            # transition was accidentally removed during a refactor.
            yield
        finally:
            os.close(fd)

    def _read_state(self) -> dict[str, Any]:
        with self.path.open("r", encoding="utf-8") as handle:
            state = json.load(handle)
        if state.get("version") != 1 or not isinstance(state.get("jobs"), dict):
            raise ValueError("unsupported scheduler state")
        if not isinstance(state.get("occurrences"), dict):
            raise ValueError("invalid occurrence store")
        return state

    def _write_state(self, state: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(state, handle, sort_keys=True, separators=(",", ":"))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    @contextmanager
    def _transaction(self) -> Iterator[dict[str, Any]]:
        with self._lock_fd():
            state = self._read_state()
            yield state
            self._write_state(state)

    @staticmethod
    def _occurrence_id(job_id: str, scheduled_at: int) -> str:
        return f"{job_id}@{scheduled_at}"

    def add_job(
        self,
        job_id: str,
        interval_seconds: int,
        first_run_at: int,
        misfire_policy: str,
        misfire_grace_seconds: int,
        max_concurrency: int,
        max_attempts: int,
        retry_backoff_seconds: int,
    ) -> None:
        if not isinstance(job_id, str) or not job_id or "@" in job_id:
            raise ValueError("job_id must be a non-empty string without @")
        self._int(interval_seconds, "interval_seconds", positive=True)
        self._int(first_run_at, "first_run_at")
        self._int(misfire_grace_seconds, "misfire_grace_seconds")
        if misfire_grace_seconds < 0:
            raise ValueError("misfire_grace_seconds must not be negative")
        self._int(max_concurrency, "max_concurrency", positive=True)
        self._int(max_attempts, "max_attempts", positive=True)
        self._int(retry_backoff_seconds, "retry_backoff_seconds", positive=True)
        if misfire_policy not in {"catch_up", "coalesce", "skip"}:
            raise ValueError("unknown misfire_policy")
        with self._transaction() as state:
            if job_id in state["jobs"]:
                raise ValueError(f"job already exists: {job_id}")
            state["jobs"][job_id] = {
                "interval_seconds": interval_seconds,
                "next_run_at": first_run_at,
                "misfire_policy": misfire_policy,
                "misfire_grace_seconds": misfire_grace_seconds,
                "max_concurrency": max_concurrency,
                "max_attempts": max_attempts,
                "retry_backoff_seconds": retry_backoff_seconds,
            }

    @staticmethod
    def _new_occurrence(
        job_id: str, scheduled_at: int, status: str, available_at: int
    ) -> dict[str, Any]:
        return {
            "job_id": job_id,
            "scheduled_at": scheduled_at,
            "available_at": available_at,
            "status": status,
            "attempt": 0,
            "owner": None,
            "token": None,
            "lease_expires_at": None,
            "last_error": None,
            "skip_reason": None,
        }

    def _recover_expired(self, state: dict[str, Any], now: int) -> None:
        for occurrence_id in sorted(state["occurrences"]):
            occurrence = state["occurrences"][occurrence_id]
            expiry = occurrence.get("lease_expires_at")
            # BUG: equality is still treated as a valid lease.
            if occurrence.get("status") != "leased" or expiry is None or expiry >= now:
                continue
            job = state["jobs"][occurrence["job_id"]]
            occurrence["owner"] = None
            occurrence["token"] = None
            occurrence["lease_expires_at"] = None
            if occurrence["attempt"] >= job["max_attempts"]:
                occurrence["status"] = "failed"
                occurrence["last_error"] = "attempts_exhausted"
            else:
                occurrence["status"] = "runnable"
                occurrence["available_at"] = now

    def _materialize(self, state: dict[str, Any], now: int) -> None:
        for job_id in sorted(state["jobs"]):
            job = state["jobs"][job_id]
            due: list[int] = []
            cursor = job["next_run_at"]
            while cursor <= now:
                due.append(cursor)
                cursor += job["interval_seconds"]
            # BUG: downtime is incorrectly folded into the next cadence point,
            # causing schedule drift after a delayed poll.
            job["next_run_at"] = now + job["interval_seconds"]
            if not due:
                continue

            policy = job["misfire_policy"]
            eligible: list[int] = []
            for scheduled_at in due:
                occurrence_id = self._occurrence_id(job_id, scheduled_at)
                if occurrence_id in state["occurrences"]:
                    continue
                lateness = now - scheduled_at
                if scheduled_at == now:
                    eligible.append(scheduled_at)
                elif lateness > job["misfire_grace_seconds"]:
                    state["occurrences"][occurrence_id] = self._new_occurrence(
                        job_id, scheduled_at, "skipped", now
                    )
                    state["occurrences"][occurrence_id]["skip_reason"] = (
                        "misfire_grace_exceeded"
                    )
                elif policy == "skip":
                    state["occurrences"][occurrence_id] = self._new_occurrence(
                        job_id, scheduled_at, "skipped", now
                    )
                    state["occurrences"][occurrence_id]["skip_reason"] = "skipped"
                else:
                    eligible.append(scheduled_at)

            if policy == "coalesce" and eligible:
                # BUG: coalescing the oldest event replays stale work instead of
                # representing the latest missed occurrence.
                selected = min(eligible)
                for scheduled_at in eligible:
                    occurrence_id = self._occurrence_id(job_id, scheduled_at)
                    if scheduled_at == selected:
                        state["occurrences"][occurrence_id] = self._new_occurrence(
                            job_id, scheduled_at, "runnable", scheduled_at
                        )
                    else:
                        state["occurrences"][occurrence_id] = self._new_occurrence(
                            job_id, scheduled_at, "skipped", now
                        )
                        state["occurrences"][occurrence_id]["skip_reason"] = "coalesced"
            elif policy == "catch_up":
                for scheduled_at in eligible:
                    occurrence_id = self._occurrence_id(job_id, scheduled_at)
                    state["occurrences"][occurrence_id] = self._new_occurrence(
                        job_id, scheduled_at, "runnable", scheduled_at
                    )
            elif policy == "skip":
                for scheduled_at in eligible:
                    occurrence_id = self._occurrence_id(job_id, scheduled_at)
                    state["occurrences"][occurrence_id] = self._new_occurrence(
                        job_id, scheduled_at, "runnable", scheduled_at
                    )

    @staticmethod
    def _claim_view(occurrence_id: str, occurrence: dict[str, Any]) -> dict[str, Any]:
        return {
            "occurrence_id": occurrence_id,
            "job_id": occurrence["job_id"],
            "scheduled_at": occurrence["scheduled_at"],
            "attempt": occurrence["attempt"],
            "token": occurrence["token"],
            "lease_expires_at": occurrence["lease_expires_at"],
        }

    def poll(
        self, worker_id: str, now: int, lease_seconds: int, limit: int = 10
    ) -> list[dict[str, Any]]:
        if not isinstance(worker_id, str) or not worker_id:
            raise ValueError("worker_id must be a non-empty string")
        self._int(now, "now")
        self._int(lease_seconds, "lease_seconds", positive=True)
        self._int(limit, "limit")
        if limit < 0:
            raise ValueError("limit must not be negative")
        with self._transaction() as state:
            self._recover_expired(state, now)
            self._materialize(state, now)
            running: dict[str, int] = {job_id: 0 for job_id in state["jobs"]}
            for occurrence in state["occurrences"].values():
                if (
                    occurrence["status"] == "leased"
                    and occurrence["lease_expires_at"] is not None
                    and occurrence["lease_expires_at"] > now
                ):
                    running[occurrence["job_id"]] += 1

            candidates = [
                (occurrence_id, occurrence)
                for occurrence_id, occurrence in state["occurrences"].items()
                if occurrence["status"] == "runnable" and occurrence["available_at"] <= now
            ]
            candidates.sort(
                key=lambda item: (
                    item[1]["available_at"],
                    item[1]["scheduled_at"],
                    item[1]["job_id"],
                    item[0],
                )
            )
            claimed: list[dict[str, Any]] = []
            for occurrence_id, occurrence in candidates:
                if len(claimed) >= limit:
                    break
                job = state["jobs"][occurrence["job_id"]]
                if running[occurrence["job_id"]] >= job["max_concurrency"]:
                    continue
                if occurrence["attempt"] >= job["max_attempts"]:
                    occurrence["status"] = "failed"
                    occurrence["last_error"] = "attempts_exhausted"
                    continue
                occurrence["attempt"] += 1
                occurrence["status"] = "leased"
                occurrence["owner"] = worker_id
                occurrence["token"] = f"{occurrence_id}#{occurrence['attempt']}"
                occurrence["lease_expires_at"] = now + lease_seconds
                running[occurrence["job_id"]] += 1
                claimed.append(self._claim_view(occurrence_id, occurrence))
            return claimed

    def heartbeat(
        self,
        occurrence_id: str,
        worker_id: str,
        token: str,
        now: int,
        lease_seconds: int,
    ) -> bool:
        self._int(now, "now")
        self._int(lease_seconds, "lease_seconds", positive=True)
        with self._transaction() as state:
            occurrence = state["occurrences"].get(occurrence_id)
            if not self._owns_valid_lease(occurrence, worker_id, token, now):
                return False
            occurrence["lease_expires_at"] = now + lease_seconds
            return True

    def complete(self, occurrence_id: str, worker_id: str, token: str, now: int) -> bool:
        self._int(now, "now")
        with self._transaction() as state:
            occurrence = state["occurrences"].get(occurrence_id)
            if not self._owns_valid_lease(occurrence, worker_id, token, now):
                return False
            occurrence["status"] = "succeeded"
            occurrence["owner"] = None
            occurrence["token"] = None
            occurrence["lease_expires_at"] = None
            occurrence["last_error"] = None
            return True

    def fail(
        self,
        occurrence_id: str,
        worker_id: str,
        token: str,
        now: int,
        retryable: bool = True,
    ) -> bool:
        self._int(now, "now")
        if not isinstance(retryable, bool):
            raise ValueError("retryable must be boolean")
        with self._transaction() as state:
            occurrence = state["occurrences"].get(occurrence_id)
            if not self._owns_valid_lease(occurrence, worker_id, token, now):
                return False
            job = state["jobs"][occurrence["job_id"]]
            attempt = occurrence["attempt"]
            occurrence["owner"] = None
            occurrence["token"] = None
            occurrence["lease_expires_at"] = None
            occurrence["last_error"] = "retryable" if retryable else "permanent"
            if retryable and attempt < job["max_attempts"]:
                occurrence["status"] = "runnable"
                occurrence["available_at"] = now + job["retry_backoff_seconds"] * (2 ** (attempt - 1))
            else:
                occurrence["status"] = "failed"
            return True

    @staticmethod
    def _owns_valid_lease(
        occurrence: dict[str, Any] | None, worker_id: str, token: str, now: int
    ) -> bool:
        return bool(
            occurrence
            and occurrence.get("status") == "leased"
            and occurrence.get("owner") == worker_id
            and occurrence.get("lease_expires_at") is not None
            # BUG: owner-only fencing accepts an old token until strictly after expiry.
            and occurrence["lease_expires_at"] >= now
        )

    def snapshot(self) -> dict[str, Any]:
        with self._lock_fd():
            return copy.deepcopy(self._read_state())
