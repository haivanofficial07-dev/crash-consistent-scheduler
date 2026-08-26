# Requirement → test traceability matrix

| # | Requirement / disclosure | Valid evidence | Plausible wrong implementation | Boundary / structure cases | Hidden variation |
|---|---|---|---|---|---|
| 1 | State is persisted as JSON and reloaded by a new `Scheduler(path)`; writes use the sidecar lock and atomic replace. | `test_restart_persists_state` | Keep state only in memory or overwrite an old snapshot. | Reopen after claim and after completion. | Different temporary paths and job IDs. |
| 2 | `poll` recovers leases when `lease_expires_at <= now`; valid leases remain owned. | `test_expiry_boundary_and_recovery` | Use `<` and leave equality leased; reclaim early. | `now == expires_at`, `now == available_at`. | Lease lengths and worker names. |
| 3 | A claim atomically sets owner, token, expiry and increments attempt; only one worker can claim under concurrency. | `test_atomic_claim_and_concurrency` | Read-then-claim without a shared lock. | Two processes poll one occurrence. | Job IDs, limits, and poll ordering. |
| 4 | Completion, heartbeat, and failure require current owner/token and an unexpired lease. | `test_stale_completion_heartbeat_failure_rejected` | Check owner only or accept expired token. | Old worker after reclaim; exact expiry. | Operation order and lease durations. |
| 5 | Retry keeps the same occurrence ID, increments the next claim attempt, and uses `backoff * 2**(attempt-1)`; exhaustion is terminal. | `test_retry_identity_backoff_and_exhaustion` | Create a new occurrence, use linear/no backoff, or retry forever. | First retry at `now + base`; last attempt. | Bases, max attempts, and failure kinds. |
| 6 | Recurrence advances from scheduled timestamps and is not advanced again by retry or completion. | `test_recurrence_no_drift_or_double_advance` | Set next run from completion/poll time or advance on completion. | Delayed completion and retry. | Intervals and downtime. |
| 7 | Misfire policies: catch_up creates each eligible overdue occurrence; coalesce creates only the latest eligible one and marks the others skipped; skip marks overdue occurrences skipped. Grace-exceeded due times are skipped. | `test_misfire_policies_and_grace` | Treat all policies alike, replay all missed work, or use `>=` grace. | Grace equality, multiple intervals, exact-now due. | Policy, interval, grace, and downtime. |
| 8 | Per-job concurrency counts only leased occurrences whose expiry is strictly greater than now; expired leases do not consume capacity. | `test_concurrency_reclaims_expired_leases` | Count all historical leased rows or count expired rows. | One valid + one expired lease at limit. | Limits and worker order. |
| 9 | Candidates are deterministic and limited by `(available_at, scheduled_at, job_id, occurrence_id)`; repeated polling is idempotent. | `test_poll_order_and_idempotence` | Iterate dict/set order or duplicate materialization. | Equal due times and repeated polls. | Job insertion order and IDs. |
| 10 | Agent modifies the exact real `/app/scheduler/scheduler.py` path. | `test_source_path_is_real_and_inside_app` | Leave a symlink or emit a different artifact. | Symlink/path escape and missing file. | Clean verifier containers. |

## Decisive values audit

All values below are disclosed in `environment/data/SPEC.md`, which is copied to
`/app/scheduler/SPEC.md` and is agent-readable. The verifier uses no private constant that the
agent cannot derive from that specification.

| Value | Source | Used by |
|---|---|---|
| timestamps are integer seconds; `expires_at <= now` is expired | SPEC §2, §5 | recovery and fencing |
| occurrence ID is `job_id@scheduled_at` | SPEC §3 | idempotence and retry identity |
| attempt starts at 1 and token is `occurrence_id#attempt` | SPEC §4 | claim/fencing |
| retry delay is `base * 2**(attempt-1)` | SPEC §6 | retry scheduling |
| grace is inclusive (`lateness <= grace`) | SPEC §7 | misfire classification |
| ordering is `(available_at, scheduled_at, job_id, occurrence_id)` | SPEC §8 | deterministic polling |

## Sound alternatives

An implementation may use a different class layout, JSON formatting, lock-file naming, or
internal helper structure. Tests interact only with the documented API and observable state, so a
different sound serialization/locking implementation still passes.

## Mutation coverage plan

The local audit mutates the solution/reference behavior into a stub, wrong expiry inequality,
owner-only fencing, retry-created occurrence, completion-time recurrence, all-policy catch-up,
expired-concurrency counting, unordered polling, and a hardcoded single trace. Each must fail at
least one independent assertion; malformed/symlink source paths are rejected before execution.
