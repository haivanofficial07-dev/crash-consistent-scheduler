Repair the persistent scheduler in `/app/scheduler/scheduler.py`. The normative contract and
documented Python API are in `/app/scheduler/SPEC.md`; read it before changing the code. The
implementation must remain standard-library-only and importable as `Scheduler` from that file.

The scheduler is used by multiple worker processes against one JSON state path. Make its durable
state transitions correct across process crashes, restarts, repeated polls, lease expiry, retries,
and recurring schedule downtime. Preserve the documented API and return shapes. The verifier will
construct fresh `Scheduler` instances against the same state file and will use more than one
worker process; all times are supplied by callers, so behavior must not depend on the wall clock.

Deliver the repaired source at exactly `/app/scheduler/scheduler.py`. It must persist the state
file, coordinate claims atomically, fence stale owners, recover expired work, apply the three
documented misfire policies, honor retry identity/backoff and concurrency limits, and keep the
recurring cadence anchored to scheduled timestamps. Do not add a separate service or change the
public API.
