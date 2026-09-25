
**Context:**
Build a small outbound call campaign dispatcher in Python. You must use `asyncio` (Python 3.11+) to handle the concurrent I/O-bound wait times efficiently.

**Core Architecture:**

1. **The Ready Queue:** An `asyncio.Queue` that holds job items.
2. **Worker Pool (Requirement 1 - Concurrency):** Create a fixed pool of $N$ worker coroutines (where $N$ is configurable). Because only $N$ workers exist and pull from the queue, this structurally guarantees at most $N$ calls are ever in flight. Calls beyond $N$ naturally wait in the queue.
3. **The Scheduler:** A separate single coroutine that manages a "Retry Heap" (min-heap keyed by `due_at` timestamp). It sleeps until the earliest `due_at`, then pushes jobs back onto the Ready Queue so workers are never blocked by `asyncio.sleep()`.

**Inputs & Mock Provider:**

1. **Cohort (Requirement 6):** A driver script (`run_campaign.py`) must generate a synthetic cohort of **200–500 contacts**. Each record must have at least: `contact_id`, `phone_number`, `name`, and `account_info`.
2. **Mock Provider:** Write exactly this function: `async def dispatch_call(phone_number) -> str`. It must inject a random `asyncio.sleep()` between **0.1 and 2 seconds**. It randomly returns one of: `answered`, `no_answer`, `voicemail`, `busy`, or `failed`. **Crucial:** Define the probability distribution for these outcomes and explicitly document it in a docstring or comment.

**Execution Rules:**

1. **Idempotency (Requirement 2):** We must prevent double-calls.
* Every job gets its `attempt_no` assigned *at creation* (or by the retry policy), not dynamically inside the worker.
* Use SQLite for persistence (`store.py`). Wrap all DB calls in `asyncio.to_thread` so they don't block the async loop.
* The `attempts` table must use a composite primary key: `(campaign_id, contact_id, attempt_no)`.
* Before dialing, the worker must execute an atomic claim (e.g., `INSERT`). If it catches an `IntegrityError` (a duplicate dispatch), it skips the job. Write a short comment explaining this mechanism.


2. **Retry Logic (Requirement 3):** Implement a `policy.py` module.
* `answered`: Terminal (Goal achieved). No retry.
* `voicemail`: Terminal. (Justification: message delivered, repeat dialing is a compliance risk).
* `no_answer` / `failed`: Retry up to a **configurable max attempts** using exponential backoff + random jitter.
* `busy`: Retry, but with a shorter, separate retry budget and faster backoff.


3. **Persistence (Requirement 4):** The SQLite database MUST log every attempt. The `attempts` table must strictly include these columns: `contact_id`, `attempt_no`, `dispatched_at` (timestamp), `disposition`, and `latency_ms` (the provider call duration only).

**Analytics Layer (Requirement 5):**
Write a module (`analytics.py`) with SQL queries that run against the SQLite database to calculate and return:

1. **Overall connection rate:** `(contacts with at least one 'answered' attempt) / (total unique contacts actually dispatched)`.
2. **Disposition breakdown:** Count and percentage per disposition across all *completed* attempts.
3. **Average attempts to resolution:** For resolved contacts (answered, voicemail, or exhausted retries), compute the average of their `MAX(attempt_no)`.
4. **Average time-to-first-connect:** For answered contacts only. `(completed_at of the answered attempt) - (first dispatched_at for that contact)`. This is wall-clock time and must include retry backoff waiting.

**Final Deliverable:**
Ensure `run_campaign.py` runs the entire flow end-to-end, includes a deliberate demo of the idempotency mechanism (submitting the exact same job twice concurrently to prove only one dials), and finally prints the 4 analytics metrics to the console.