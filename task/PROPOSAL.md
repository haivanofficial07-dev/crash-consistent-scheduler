# Crash-Consistent Distributed Job Scheduler Recovery

## 1. The difficulty

The task repairs a persistent scheduler used by several workers. It is difficult because one
state machine must make lease ownership, expiry, fencing tokens, retry attempts, recurring
cadence, misfire policy, and per-job concurrency agree across crashes and repeated polling. A
locally plausible repair can still duplicate a logical occurrence, accept a stale completion,
drift a recurring schedule, or let an expired lease consume capacity. The fixture is synthetic,
but models durable state and worker races seen in production schedulers. It is realistic work for
an engineer debugging distributed job execution and recovery semantics. Difficulty comes from
the interacting transitions and boundary cases, not from large data or timing pressure.

## 2. The intended approach

Treat the JSON state file as the durable source of truth and serialize every read-modify-write
transition with the sidecar lock. Materialize due timestamps from the stored `next_run_at`, then
advance that pointer from the scheduled timestamp rather than wall-clock completion time. Recover
leases at `expires_at <= now`, count only still-valid leases, and claim runnable occurrences while
holding the same lock. Every claim increments the attempt and creates a new token; heartbeat,
completion, and failure require the current owner, token, and unexpired lease. Retry returns the
same logical occurrence to runnable after exponential backoff, while recurrence remains separate.
Misfire policy is applied once as each due timestamp is materialized. A senior engineer holding
the model should need about 6 focused hours: state-machine reconstruction, implementation,
concurrency/recovery testing, and repair of boundary interactions.

## 3. How it will be verified

The verifier guards and reads `/app/scheduler/scheduler.py`, copies it into a minimal private
Python chroot, and runs it as an unprivileged user with scenario input over stdin. The submitted
process cannot see the `/tests` mount or hidden fixtures. Hidden traces assert exact state
transitions and returned claim fields; they do not execute the reference solution or compare
source text. Truth is encoded in test-side scenario expectations and the normative agent-visible
specification, while hidden scenario parameters vary job IDs, intervals, policies, downtime, lease
lengths, and worker order. The verifier checks the expiry equality boundary, stale-operation
rejection, atomic single ownership, retry identity/backoff, recurrence cadence, restart
persistence, misfire/coalesce/skip behavior, and concurrency recovery. The artifact is code, so
there are no numeric tolerances: malformed paths, symlinks, malformed API results, and any wrong
state are rejected exactly.

## 4. Category and sub-category

This is Systems Infrastructure and Operations because the artifact is a durable worker scheduler
and the repair concerns ownership, recovery, and service operation. Scheduling and automation
infrastructure is the direct sub-category: the core work is deciding which recurring executions
become runnable and safely coordinating workers across leases, retries, downtime, and restarts.
