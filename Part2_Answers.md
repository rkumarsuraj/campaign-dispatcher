Part 2 — Explain

1. Concurrency mechanism
Enforced by a fixed pool of N worker coroutines, not a semaphore. Each worker loops: pull a job, claim it in the database, dial, record, hand off to the retry policy, repeat. With exactly N workers, at most N calls can ever be in flight — the limit is structural, not a counter that can drift.

Chosen over a semaphore because that approach spawns one coroutine per contact — fine at a few hundred, but 500,000 live coroutines at scale, and it makes backlog invisible. A worker pool makes queue depth a real, loggable number instead.


2. Idempotency guarantee
The key is a database primary key: (campaign_id, contact_id, attempt_no) on the attempts table. A worker inserts an in_flight row before dialling; a duplicate insert violates the constraint and the worker discards the job without dialling.

Two duplicate requests arriving together: both enqueued, both popped by different workers. Worker A's insert succeeds — it owns the attempt. Worker B's identical insert raises an integrity error; Worker B discards the job. No second call is placed. The check and the claim are one atomic write, so there's no window for a race.

Critically, attempt_no is assigned when a job is created, never recomputed by a worker at dial time — otherwise a duplicate arriving after attempt 1 completes would get a fresh number and dial for real, bypassing the dedupe.

If removed: both workers dial, the phone rings twice, and the attempts table carries two rows for one logical attempt — corrupting every downstream metric.


3. Retry/backoff design
| Disposition | Retried? | Why |
|---|---|---|
| `no_answer`, `failed` | Yes, full budget | Nothing delivered; try again later |
| `busy` | Yes, shorter budget | Line is live, but rapid redial of an engaged line is a compliance risk |
| `voicemail` | No | Call connected, message delivered — redialling adds nothing and is a BFSI compliance concern; handled as a channel hand-off instead |
| `answered` | No | Success |

Backoff: delay = min(base * 2^(attempt-1), cap) * random(0.5, 1.5) — exponential to give a struggling provider room, capped so late attempts don't schedule hours out, jittered so a batch that fails together doesn't retry together and re-saturate the channel cap.

4. Scaling this up
Two things break first at 500K contacts, horizontally scaled, against a hard provider cap:

    1.	Concurrency limit becomes per-process — 10 instances × N workers = 10N calls against a cap of N. Fix: a distributed lease (Redis token bucket, TTL-based) shared across instances.
    2.	SQLite is single-writer. Fix: Postgres, keeping the same unique-constraint idempotency logic unchanged; stream completed attempts to a separate analytics store.

Follow-on: the in-memory retry heap doesn't survive a restart or span instances — becomes a next_attempt_at column polled with SKIP LOCKED.

5. Production gaps
    1.	No webhook path. A real provider returns a call SID immediately and reports the outcome asynchronously. Inverts the design — a worker would release its channel at dial time, and attempt state would need reconciliation for webhooks that never arrive.
    2.	No permanent-vs-transient failure classification. All failed is retried identically since the mock has no reason codes. Needs carrier reason codes, a DNC/consent check, and a circuit breaker distinct from per-contact retry.


6. AI assistant use
Used an AI coding assistant, working from a written spec before any code existed. Corrections made at the design-review stage, before implementation:

-	attempt_no allocation — left unspecified, it would've been computed inside the worker at dial time, silently breaking the idempotency guarantee against a late duplicate. Fixed by assigning it once, at job creation.
-	Blocking sqlite3 calls inside asyncio — the first draft called it directly from coroutines, freezing the whole event loop per write. Moved to asyncio.to_thread with WAL mode.
-	Termination check — "queue empty, heap empty, nothing in flight" is race-prone. Replaced with a single unresolved-contact counter, decremented once per resolved contact.
-	No way to demonstrate idempotency — a clean run never dispatches a contact twice, so the dedupe path would never execute. Added a deliberate duplicate submission and a 50-concurrent-claims test to verify it under real pressure.


