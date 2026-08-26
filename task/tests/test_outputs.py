"""Behavioral verifier for the persistent scheduler contract.

The submitted module is copied out of /app and exercised as an unprivileged process. Scenario
data and expected transitions live here, not in the agent image. Tests use the documented API and
accept any implementation with the same observable state-machine behavior.
"""

from __future__ import annotations

import json
import math
import os
import pwd
import shutil
import stat
import subprocess
import sys
import sysconfig
import tempfile
from pathlib import Path
from typing import Any

import pytest


SOURCE = Path("/app/scheduler/scheduler.py")
SPEC = Path("/app/scheduler/SPEC.md")
CLAIM_KEYS = {
    "occurrence_id",
    "job_id",
    "scheduled_at",
    "attempt",
    "token",
    "lease_expires_at",
}
_RUNTIME_ROOT: Path | None = None
_NOBODY = pwd.getpwnam("nobody")
_JAIL_ENV = {"PATH": "/usr/local/bin:/usr/bin", "LD_LIBRARY_PATH": "/usr/local/lib:/lib"}


def _guarded_read(path: Path) -> str:
    """Read a graded path only when it is a regular file inside /app."""
    assert not path.is_symlink(), f"{path} must not be a symlink"
    resolved = Path(os.path.realpath(path))
    assert str(resolved).startswith("/app/"), f"{path} escapes /app: {resolved}"
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        mode = os.fstat(fd).st_mode
        assert stat.S_ISREG(mode), f"{path} must be a regular file"
        chunks: list[bytes] = []
        while True:
            block = os.read(fd, 65536)
            if not block:
                break
            chunks.append(block)
        return b"".join(chunks).decode("utf-8")
    finally:
        os.close(fd)


def _drop_to_nobody() -> None:
    """Drop submitted-code subprocesses to the image's nobody account."""
    if os.geteuid() != 0:
        return
    os.setgroups([])
    os.setgid(_NOBODY.pw_gid)
    os.setuid(_NOBODY.pw_uid)


def _chroot_and_drop() -> None:
    """Enter the private runtime jail before dropping privileges for submitted code."""
    if os.geteuid() == 0:
        assert _RUNTIME_ROOT is not None
        os.chroot(str(_RUNTIME_ROOT))
        os.chdir("/")
    _drop_to_nobody()


def _copy_runtime_file(source: Path, root: Path) -> None:
    """Copy one runtime dependency while preserving its absolute path below the jail."""
    destination = root / source.relative_to("/")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    resolved = source.resolve()
    if resolved != source:
        resolved_destination = root / resolved.relative_to("/")
        resolved_destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(resolved, resolved_destination)


def _ensure_runtime() -> Path:
    """Build one minimal Python chroot so submitted code cannot see the /tests mount."""
    global _RUNTIME_ROOT
    if _RUNTIME_ROOT is not None:
        return _RUNTIME_ROOT
    root = Path(tempfile.mkdtemp(prefix="scheduler-jail-", dir="/tmp"))
    os.chmod(root, 0o755)
    (root / "runs").mkdir()
    os.chmod(root / "runs", 0o777)
    executable = Path(sys.executable).resolve()
    _copy_runtime_file(executable, root)
    stdlib = Path(sysconfig.get_path("stdlib")).resolve()
    shutil.copytree(stdlib, root / stdlib.relative_to("/"), symlinks=True)
    for line in subprocess.check_output(["ldd", str(executable)], text=True).splitlines():
        fields = line.strip().split()
        candidates = [field for field in fields if field.startswith("/")]
        if candidates:
            dependency = Path(candidates[0])
            if dependency.exists():
                _copy_runtime_file(dependency, root)
    _RUNTIME_ROOT = root
    return root


def _copy_source() -> tuple[Path, Path]:
    source_text = _guarded_read(SOURCE)
    runtime = _ensure_runtime()
    work = Path(tempfile.mkdtemp(prefix="scheduler-case-", dir=runtime / "runs"))
    os.chmod(work, 0o777)
    module_dir = work / "module"
    module_dir.mkdir()
    module_dir.joinpath("scheduler.py").write_text(source_text, encoding="utf-8")
    os.chmod(module_dir, 0o755)
    os.chmod(module_dir / "scheduler.py", 0o644)
    return work, module_dir


DRIVER = r'''
import json
import os
import sys

module_dir, state_path = sys.argv[1:3]
assert not os.path.exists("/tests"), "submitted code must not see the verifier mount"
sys.path.insert(0, module_dir)
from scheduler import Scheduler

payload = json.load(sys.stdin)
scheduler = None
results = []
for operation in payload:
    name = operation["name"]
    if operation.get("reload"):
        scheduler = Scheduler(state_path)
    args = operation.get("args", [])
    kwargs = operation.get("kwargs", {})
    if name == "create":
        scheduler = Scheduler.create(state_path)
        results.append(None)
    else:
        if scheduler is None:
            scheduler = Scheduler(state_path)
        value = getattr(scheduler, name)(*args, **kwargs)
        results.append(value)
print(json.dumps(results, sort_keys=True))
'''


def _run(module_dir: Path, state_path: Path, operations: list[dict[str, Any]]) -> list[Any]:
    runtime = _ensure_runtime()
    command = [
        sys.executable,
        "-I",
        "-c",
        DRIVER,
        "/" + str(module_dir.relative_to(runtime)),
        "/" + str(state_path.relative_to(runtime)),
    ]
    result = subprocess.run(
        command,
        input=json.dumps(operations).encode("utf-8"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=_chroot_and_drop,
        env=_JAIL_ENV,
        check=False,
    )
    assert result.returncode == 0, result.stderr.decode("utf-8", "replace")
    return json.loads(result.stdout.decode("utf-8"))


def _claim(result: Any) -> dict[str, Any]:
    assert isinstance(result, list)
    assert len(result) == 1
    claim = result[0]
    assert isinstance(claim, dict)
    assert set(claim) == CLAIM_KEYS
    for key in ("scheduled_at", "attempt", "lease_expires_at"):
        assert isinstance(claim[key], int) and not isinstance(claim[key], bool)
        assert math.isfinite(claim[key])
    for key in ("occurrence_id", "job_id", "token"):
        assert isinstance(claim[key], str)
    return claim


def _job(
    job_id: str,
    interval: int = 10,
    first: int = 0,
    policy: str = "catch_up",
    grace: int = 100,
    concurrency: int = 4,
    attempts: int = 4,
    backoff: int = 5,
) -> dict[str, Any]:
    return {
        "name": "add_job",
        "args": [job_id, interval, first, policy, grace, concurrency, attempts, backoff],
    }


def _setup(module_dir: Path, state: Path, *jobs: dict[str, Any]) -> None:
    _run(module_dir, state, [{"name": "create"}, *jobs])


def test_source_path_is_real_and_inside_app():
    """The requested deliverable is a real UTF-8 Python file at the documented /app path."""
    text = _guarded_read(SOURCE)
    assert text.strip()
    assert SPEC.is_file() and not SPEC.is_symlink()


def test_expiry_boundary_and_fencing():
    """Equality at expiry is stale, and old owner/token operations cannot affect a reclaim."""
    work, module = _copy_source()
    try:
        state = work / "state.json"
        _setup(module, state, _job("billing", interval=100, concurrency=1, attempts=4))
        first = _claim(_run(module, state, [{"name": "poll", "args": ["worker-a", 0, 10, 1]}])[0])
        assert first["occurrence_id"] == "billing@0"
        assert first["attempt"] == 1 and first["lease_expires_at"] == 10
        assert _run(module, state, [{"name": "complete", "args": ["billing@0", "worker-a", first["token"], 10]}])[0] is False
        assert _run(module, state, [{"name": "heartbeat", "args": ["billing@0", "worker-a", first["token"], 10, 5]}])[0] is False
        second = _claim(_run(module, state, [{"name": "poll", "args": ["worker-b", 10, 5, 1]}])[0])
        assert second["occurrence_id"] == first["occurrence_id"]
        assert second["attempt"] == 2 and second["token"] != first["token"]
        assert _run(module, state, [{"name": "complete", "args": ["billing@0", "worker-a", first["token"], 11]}])[0] is False
        assert _run(module, state, [{"name": "fail", "args": ["billing@0", "worker-a", first["token"], 11, True]}])[0] is False
        assert _run(module, state, [{"name": "heartbeat", "args": ["billing@0", "worker-b", second["token"], 11, 5]}])[0] is True
        assert _run(module, state, [{"name": "complete", "args": ["billing@0", "worker-b", second["token"], 12]}])[0] is True

        same_state = work / "same-worker.json"
        _setup(module, same_state, _job("same", interval=100, concurrency=1, attempts=3))
        old = _claim(_run(module, same_state, [{"name": "poll", "args": ["worker-a", 0, 10, 1]}])[0])
        current = _claim(_run(module, same_state, [{"name": "poll", "args": ["worker-a", 10, 5, 1]}])[0])
        assert current["attempt"] == 2 and current["token"] != old["token"]
        assert _run(module, same_state, [{"name": "complete", "args": ["same@0", "worker-a", old["token"], 11]}])[0] is False
        assert _run(module, same_state, [{"name": "heartbeat", "args": ["same@0", "worker-a", old["token"], 11, 5]}])[0] is False
        assert _run(module, same_state, [{"name": "fail", "args": ["same@0", "worker-a", old["token"], 11, True]}])[0] is False
        assert _run(module, same_state, [{"name": "complete", "args": ["same@0", "worker-a", current["token"], 11]}])[0] is True
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_retry_identity_backoff_and_exhaustion():
    """Retry keeps one logical occurrence, uses exponential delay, and eventually becomes terminal."""
    work, module = _copy_source()
    try:
        state = work / "state.json"
        _setup(module, state, _job("retry", interval=100, attempts=4, backoff=5))
        first = _claim(_run(module, state, [{"name": "poll", "args": ["w", 0, 30, 1]}])[0])
        assert _run(module, state, [{"name": "fail", "args": ["retry@0", "w", first["token"], 0, True]}])[0] is True
        assert _run(module, state, [{"name": "poll", "args": ["w", 4, 30, 1]}])[0] == []
        second = _claim(_run(module, state, [{"name": "poll", "args": ["w2", 5, 30, 1]}])[0])
        assert second["occurrence_id"] == first["occurrence_id"] and second["attempt"] == 2
        assert _run(module, state, [{"name": "fail", "args": ["retry@0", "w2", second["token"], 5, True]}])[0] is True
        assert _run(module, state, [{"name": "poll", "args": ["w", 14, 30, 1]}])[0] == []
        third = _claim(_run(module, state, [{"name": "poll", "args": ["w3", 15, 30, 1]}])[0])
        assert third["occurrence_id"] == first["occurrence_id"] and third["attempt"] == 3
        assert _run(module, state, [{"name": "fail", "args": ["retry@0", "w3", third["token"], 15, True]}])[0] is True
        assert _run(module, state, [{"name": "poll", "args": ["w", 34, 30, 1]}])[0] == []
        fourth = _claim(_run(module, state, [{"name": "poll", "args": ["w4", 35, 30, 1]}])[0])
        assert fourth["occurrence_id"] == first["occurrence_id"] and fourth["attempt"] == 4
        assert _run(module, state, [{"name": "fail", "args": ["retry@0", "w4", fourth["token"], 35, True]}])[0] is True
        final = _run(module, state, [{"name": "snapshot"}])[0]["occurrences"]["retry@0"]
        assert final["status"] == "failed" and final["attempt"] == 4
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_recurrence_no_drift_or_double_advance():
    """Recurring cadence follows scheduled timestamps, not completion or retry wall time."""
    work, module = _copy_source()
    try:
        state = work / "state.json"
        _setup(module, state, _job("cadence", interval=10, first=0, concurrency=8))
        at_zero = _claim(_run(module, state, [{"name": "poll", "args": ["w", 0, 20, 1]}])[0])
        assert _run(module, state, [{"name": "complete", "args": ["cadence@0", "w", at_zero["token"], 7]}])[0] is True
        assert _run(module, state, [{"name": "poll", "args": ["w", 7, 20, 1]}])[0] == []
        at_ten = _claim(_run(module, state, [{"name": "poll", "args": ["w", 10, 20, 1]}])[0])
        assert at_ten["occurrence_id"] == "cadence@10"
        snapshot = _run(module, state, [{"name": "snapshot"}])[0]
        assert snapshot["jobs"]["cadence"]["next_run_at"] == 20

        late_state = work / "late.json"
        _setup(module, late_state, _job("late", interval=10, first=0, concurrency=8))
        claims = _run(module, late_state, [{"name": "poll", "args": ["w", 35, 20, 8]}])[0]
        assert [item["occurrence_id"] for item in claims] == ["late@0", "late@10", "late@20", "late@30"]
        late_snapshot = _run(module, late_state, [{"name": "snapshot"}])[0]
        assert late_snapshot["jobs"]["late"]["next_run_at"] == 40

        coupled_state = work / "coupled.json"
        _setup(module, coupled_state, _job("coupled", interval=10, first=0, concurrency=4, attempts=3, backoff=5))
        original = _claim(_run(module, coupled_state, [{"name": "poll", "args": ["w", 0, 20, 1]}])[0])
        assert _run(module, coupled_state, [{"name": "fail", "args": ["coupled@0", "w", original["token"], 0, True]}])[0] is True
        retried = _claim(_run(module, coupled_state, [{"name": "poll", "args": ["w", 5, 20, 1]}])[0])
        assert retried["occurrence_id"] == "coupled@0" and retried["attempt"] == 2
        assert _run(module, coupled_state, [{"name": "fail", "args": ["coupled@0", "w", retried["token"], 5, True]}])[0] is True
        next_occurrence = _claim(_run(module, coupled_state, [{"name": "poll", "args": ["w2", 10, 20, 1]}])[0])
        assert next_occurrence["occurrence_id"] == "coupled@10"
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_misfire_policies_and_grace():
    """Catch-up, coalesce, skip, inclusive grace, and exact-now due handling are distinct."""
    work, module = _copy_source()
    try:
        for policy in ("catch_up", "coalesce", "skip"):
            state = work / f"{policy}.json"
            _setup(module, state, _job(policy, interval=10, first=0, policy=policy, grace=15, concurrency=10))
            claims = _run(module, state, [{"name": "poll", "args": ["w", 25, 20, 10]}])[0]
            occurrences = _run(module, state, [{"name": "snapshot"}])[0]["occurrences"]
            assert occurrences[f"{policy}@0"]["status"] == "skipped"
            assert occurrences[f"{policy}@0"]["skip_reason"] == "misfire_grace_exceeded"
            assert occurrences[f"{policy}@10"]["skip_reason"] == ("coalesced" if policy == "coalesce" else ("skipped" if policy == "skip" else None))
            if policy == "catch_up":
                assert [c["occurrence_id"] for c in claims] == ["catch_up@10", "catch_up@20"]
            elif policy == "coalesce":
                assert [c["occurrence_id"] for c in claims] == ["coalesce@20"]
                assert occurrences["coalesce@20"]["status"] == "leased"
            else:
                assert claims == []
                assert occurrences["skip@20"]["status"] == "skipped"
            assert occurrences[f"{policy}@10"]["status"] != "leased" if policy != "catch_up" else True

        exact_state = work / "exact.json"
        _setup(module, exact_state, _job("exact", interval=10, first=25, policy="skip", grace=0))
        exact = _run(module, exact_state, [{"name": "poll", "args": ["w", 25, 10, 1]}])[0]
        assert [c["occurrence_id"] for c in exact] == ["exact@25"]
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_concurrency_reclaims_expired_leases():
    """A valid lease consumes capacity, while an equality-expired lease is recovered first."""
    work, module = _copy_source()
    try:
        state = work / "state.json"
        _setup(module, state, _job("capacity", interval=10, first=0, concurrency=1, attempts=4))
        first = _claim(_run(module, state, [{"name": "poll", "args": ["a", 0, 10, 1]}])[0])
        assert _run(module, state, [{"name": "poll", "args": ["b", 5, 10, 1]}])[0] == []
        reclaimed = _claim(_run(module, state, [{"name": "poll", "args": ["b", 10, 10, 1]}])[0])
        assert reclaimed["occurrence_id"] == first["occurrence_id"]
        assert reclaimed["attempt"] == 2
        snapshot = _run(module, state, [{"name": "snapshot"}])[0]
        assert snapshot["occurrences"]["capacity@10"]["status"] == "runnable"
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_poll_order_and_idempotence():
    """Repeated polls do not duplicate occurrences and use the disclosed total tie-break."""
    work, module = _copy_source()
    try:
        state = work / "state.json"
        _setup(
            module,
            state,
            _job("zeta", interval=100, first=0, concurrency=1),
            _job("alpha", interval=100, first=0, concurrency=1),
        )
        claims = _run(module, state, [{"name": "poll", "args": ["w", 0, 20, 10]}])[0]
        assert [c["occurrence_id"] for c in claims] == ["alpha@0", "zeta@0"]
        assert _run(module, state, [{"name": "poll", "args": ["w2", 0, 20, 10]}])[0] == []
        snapshot = _run(module, state, [{"name": "snapshot"}])[0]
        assert sorted(snapshot["occurrences"]) == ["alpha@0", "zeta@0"]
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_restart_persists_state():
    """A fresh Scheduler instance sees durable jobs, leases, and terminal transitions."""
    work, module = _copy_source()
    try:
        state = work / "state.json"
        _setup(module, state, _job("restart", interval=50, concurrency=1))
        claim = _claim(_run(module, state, [{"name": "poll", "args": ["worker", 0, 20, 1]}])[0])
        fresh = _run(module, state, [{"name": "snapshot", "reload": True}])[0]
        assert fresh["occurrences"]["restart@0"]["owner"] == "worker"
        assert fresh["occurrences"]["restart@0"]["token"] == claim["token"]
        assert _run(module, state, [{"name": "complete", "args": ["restart@0", "worker", claim["token"], 1], "reload": True}])[0] is True
        final = _run(module, state, [{"name": "snapshot", "reload": True}])[0]
        assert final["occurrences"]["restart@0"]["status"] == "succeeded"
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_atomic_claim_and_shared_lock():
    """Concurrent processes can produce at most one claim for one runnable occurrence."""
    work, module = _copy_source()
    try:
        for round_no in range(5):
            state = work / f"race-{round_no}.json"
            _setup(module, state, _job("race", interval=100, concurrency=1, attempts=5))
            read_fd, write_fd = os.pipe()
            children: list[subprocess.Popen[bytes]] = []
            count = 24
            child_code = (
                "import os,sys,json; "
                "os.read(int(sys.argv[3]),1); "
                "assert not os.path.exists('/tests'); "
                "sys.path.insert(0,sys.argv[1]); "
                "from scheduler import Scheduler; "
                "print(json.dumps(Scheduler(sys.argv[2]).poll(sys.argv[4],0,50,1)))"
            )
            try:
                for index in range(count):
                    children.append(
                        subprocess.Popen(
                            [
                                sys.executable,
                                "-I",
                                "-c",
                                child_code,
                                "/" + str(module.relative_to(_ensure_runtime())),
                                "/" + str(state.relative_to(_ensure_runtime())),
                                str(read_fd),
                                f"w{index}",
                            ],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            pass_fds=(read_fd,),
                            preexec_fn=_chroot_and_drop,
                            env=_JAIL_ENV,
                        )
                    )
                os.write(write_fd, b"x" * count)
            finally:
                os.close(write_fd)
                os.close(read_fd)
            outputs = []
            for child in children:
                stdout, stderr = child.communicate()
                assert child.returncode == 0, stderr.decode("utf-8", "replace")
                outputs.append(json.loads(stdout.decode("utf-8")))
            claimed = [item for result in outputs for item in result]
            assert len(claimed) == 1
            assert claimed[0]["occurrence_id"] == "race@0"
    finally:
        shutil.rmtree(work, ignore_errors=True)
