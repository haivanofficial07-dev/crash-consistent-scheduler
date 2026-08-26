# Persistent scheduler contract (normative)

Repair `/app/scheduler/scheduler.py`. It must expose the standard-library-only API below. The
state file is the durable source of truth; every successful mutating call must persist it so a
new `Scheduler(same_path)` observes the prior state. A sidecar lock may be named
`<state_path>.lock`. Writes must be atomic from the point of view of another scheduler instance.

## API and state

```python
Scheduler(state_path)
Scheduler.create(state_path)
add_job(job_id, interval_seconds, first_run_at, misfire_policy,
        misfire_grace_seconds, max_concurrency, max_attempts,
        retry_backoff_seconds)
poll(worker_id, now, lease_seconds, limit=10) -> list[claim]
heartbeat(occurrence_id, worker_id, token, now, lease_seconds) -> bool
complete(occurrence_id, worker_id, token, now) -> bool
fail(occurrence_id, worker_id, token, now, retryable=True) -> bool
snapshot() -> dict
```

All times are integer seconds supplied by the caller; do not read the wall clock. `interval`,
grace, lease, concurrency, and attempts are positive integers. `misfire_policy` is one of
`catch_up`, `coalesce`, or `skip`. `max_attempts` is the total number of claims allowed for one
occurrence, including the first claim. `poll` returns at most `limit` claims and returns `[]` if
there is no eligible work.

The JSON state has `version`, `jobs`, and `occurrences`. Each job stores its configuration and
`next_run_at`. Each occurrence is keyed by its ID and stores `job_id`, `scheduled_at`,
`available_at`, `status`, `attempt`, `owner`, `token`, `lease_expires_at`, `last_error`, and
`skip_reason`. Status is one of `runnable`, `leased`, `succeeded`, `failed`, or `skipped`.

Each claim has exactly `occurrence_id`, `job_id`, `scheduled_at`, `attempt`, `token`, and
`lease_expires_at`.

## Occurrence identity and claims

For job `J` and scheduled timestamp `T`, the logical occurrence ID is `J@T`. It is created at
most once, regardless of how many times `poll` is called. A newly claimed occurrence has
`attempt = 1`; every later claim after expiry or retry increments it. Its fencing token is the
exact string `occurrence_id#attempt`. A claim sets `status=leased`, `owner`, `token`, and
`lease_expires_at=now+lease_seconds` in the same serialized transition. A valid lease has
`lease_expires_at > now`; equality means expired.

`heartbeat`, `complete`, and `fail` succeed only when the occurrence is leased, the worker and
token exactly match the current values, and the lease is valid at the supplied `now`. Otherwise
they return `False` and must not change the occurrence. A successful heartbeat replaces the
expiry with `now + lease_seconds`. Completion sets `succeeded` and clears lease ownership.

## Retry and recurrence

Retryable failure on attempt `a` with `a < max_attempts` keeps the same occurrence ID, clears its
lease, sets it runnable, and makes it eligible at `now + retry_backoff_seconds * 2**(a-1)`.
Non-retryable failure or failure on the last allowed attempt sets `failed`. Completion and retry
never create or delete a recurring occurrence and never move the recurrence pointer.

After a job's due timestamp `T` is processed, its next due timestamp is `T + interval_seconds`.
Advance from scheduled timestamps until `next_run_at > now`, independent of claim, completion,
retry, or worker wall-clock time. Thus delayed completion cannot drift the recurring cadence.

## Polling, misfires, and ordering

At the start of every `poll`, first recover every leased occurrence with
`lease_expires_at <= now`: clear owner/token/expiry, leave its attempt counter unchanged, and
make it runnable with `available_at=now` unless its attempt count has already reached
`max_attempts`, in which case mark it failed with `last_error=attempts_exhausted`. Then materialize
all due timestamps `T <= now`.

For a due timestamp with `T == now`, create a runnable occurrence under every policy. For an
overdue timestamp, lateness is `now-T`. If `lateness > misfire_grace_seconds`, mark it skipped
with reason `misfire_grace_exceeded`. Otherwise apply the job policy: `catch_up` creates every
eligible overdue occurrence; `skip` marks every overdue occurrence skipped with reason `skipped`;
`coalesce` creates only the latest eligible overdue occurrence and marks the other eligible due
timestamps skipped with reason `coalesced`. Existing occurrence IDs are never recreated or
changed by a repeated poll.

Only runnable occurrences with `available_at <= now` may be claimed. For each job, concurrency
is the number of leased occurrences with `lease_expires_at > now`; expired leases do not count
after recovery. Claim candidates are ordered globally by
`(available_at, scheduled_at, job_id, occurrence_id)`, then the first eligible candidates are
claimed until `limit` is reached or each job's concurrency is full. The ordering is a total
lexicographic ordering using integer timestamps and case-sensitive job IDs.

## Persistence and determinism

Use a shared lock for every state transition, including recovery plus claim, and write a complete
new JSON document atomically. Never rely on process-local memory or the wall clock. The verifier
may use multiple scheduler processes against one state path and may construct a fresh scheduler
between any calls.
