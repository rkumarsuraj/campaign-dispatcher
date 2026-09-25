# Outbound Voice Campaign Dispatcher

Simulated outbound calling campaign dispatcher: concurrency-capped dispatch, retry
with backoff, per-attempt persistence, and SQL analytics. No real telephony.

**Stack:** Python 3.11+, `asyncio`, `sqlite3`. Standard library only — no `pip install`.

## Run it

```bash
python run_campaign.py                                      # 300 contacts, 20 channels
python run_campaign.py --contacts 500 --concurrency 30      # larger cohort
python run_campaign.py --export attempts.csv                # dump raw attempts
python run_campaign.py --seed 0                             # non-deterministic run
python test_dispatcher.py                                   # 28 tests, no pytest needed
```

A 300-contact run takes roughly 40 seconds, most of it simulated ring time and retry
backoff.

## Files

| File | What it does |
|---|---|
| `provider.py` | Mock provider. `dispatch_call()` — random delay, weighted disposition. |
| `store.py` | SQLite schema and all DB access. The idempotency constraint lives here. |
| `policy.py` | Retry rules as a data table. Pure function, no I/O. |
| `dispatcher.py` | Ready queue, worker pool, retry heap, scheduler, termination. |
| `analytics.py` | The four required metrics, as SQL. |
| `run_campaign.py` | Driver: synthetic cohort, run, print analytics, idempotency demo. |
| `test_dispatcher.py` | Tests, including 50 concurrent claims on one key. |
| `config.py` | All knobs, persisted with the campaign row. |

## How it works

```
run_campaign.py
      │ submit(Job(contact, attempt_no=1))
      ▼
 ready queue ◄──── scheduler ◄──── retry heap (min-heap by due time)
      │                                   ▲
      ▼ get()                             │
 worker pool (N coroutines)               │
      │                                   │
      ├─► store.claim_attempt()  ── idempotency gate, returns False on duplicate
      ├─► provider.dispatch_call()
      ├─► store.record_attempt()
      └─► policy.decide() ──► RETRY(delay) ┘   or   RESOLVED ──► counter--
```

Contacts enter as jobs on a ready queue. A fixed pool of N workers pulls jobs, claims
each in the database, dials the mock provider, records the result, and asks the policy
what happens next. Retries go onto a heap with a due time; one scheduler coroutine
feeds them back onto the queue when due. The campaign ends when the unresolved-contact
counter hits zero.

## The four design decisions

**Concurrency — worker pool, not a semaphore.** N workers exist, so N calls can be in
flight. Structural, not a counter that can drift. A semaphore would mean one coroutine
per contact, which means 500,000 live coroutines at scale, and makes backlog invisible
— queue depth is a number you can log, "coroutines blocked on a permit" is not.

**Idempotency — a database primary key.** `attempts` has
`PRIMARY KEY (campaign_id, contact_id, attempt_no)`. A worker INSERTs an `in_flight`
row before dialling; a duplicate insert raises `IntegrityError` and the worker drops
the job without dialling. Check and claim are one atomic write, so there is no window
to race in.

The subtle part: `attempt_no` is assigned when a job is **created**, never computed by
a worker at dial time. If a worker did its own `MAX(attempt_no) + 1`, a duplicate
arriving after attempt 1 completed would get a fresh number and dial for real,
straight past the dedupe.

**Retry split.** `no_answer` and `failed` retry on exponential backoff with jitter.
`busy` retries on a shorter delay with a tighter budget — busy proves the line is live,
but rapid redialling of an engaged line is a pattern regulators dislike. `voicemail` is
terminal: the call connected and the message landed, so redialling adds nothing and, in
BFSI, is a compliance risk — the right move is a channel hand-off, recorded as an
outcome. `answered` is terminal success.

Jitter is the load-bearing part: without it, everything that fails in one window
retries in the same window and re-saturates the channel cap when the provider is
already unhealthy.

**Scheduler as its own coroutine.** A worker must never `await asyncio.sleep(backoff)`
— that holds one of the N phone lines idle for the whole window. Only the scheduler
waits.

## Two things worth knowing about the implementation

**`sqlite3` is blocking**, so every DB call goes through `asyncio.to_thread` with a
lock around a single WAL-mode connection. At 300 contacts, blocking the loop would be
practically invisible, but it's wrong in principle and stops being invisible once the
database is remote. That lock is also an honest preview of the scaling problem: SQLite
is single-writer.

**Termination is an unresolved-contact counter**, not "queue empty and heap empty and
nothing in flight". The three-way check is race-prone — a worker can pop a job (queue
reads empty) before it registers as in-flight.

## Analytics: the ambiguities, resolved

Each required metric had a wording ambiguity. Rather than pick silently:

1. **Connection rate** — denominator is contacts *dispatched*, not cohort size.
2. **Disposition breakdown** — per completed **attempt**, not per contact. A contact
   that went no_answer → no_answer → answered contributes three rows.
3. **Average attempts to resolution** — reported both ways: all resolved contacts, and
   the literal "answered or exhausted only" reading that omits voicemail.
4. **Time-to-first-connect** — **wall-clock**, including backoff waits, because the
   business question is "how long to reach this customer". Dial-time-only is printed
   alongside for contrast; it measures provider speed, not campaign speed.

## Sample output (300 contacts, 20 channels, seed 42)

```
1. OVERALL CONNECTION RATE
   60.33%  (181 answered / 300 contacts dispatched)

2. DISPOSITION BREAKDOWN  (per completed attempt, n=528)
   answered         181   34.28%
   no_answer        152   28.79%
   voicemail         85    16.1%
   busy              63   11.93%
   failed            47     8.9%

3. AVERAGE ATTEMPTS BEFORE RESOLUTION
   all resolved contacts      : 1.76  (n=300)
   answered or exhausted only : 1.805 (n=215)

4. AVERAGE TIME-TO-FIRST-CONNECT
   wall-clock (incl. backoff) : 8.496s
   dial time only             : 2.13s

   duplicate dispatches blocked: 1
   idempotency check           : PASS
```

Connection rate (60%) is much higher than the answered share of attempts (34%) because
retries work — a contact gets up to four shots at being reached.

## Known limitations

Deliberate, and discussed at more length in the Part 2 write-up:

- **No webhook path.** A real provider returns a call SID immediately and reports the
  outcome asynchronously. That inverts the design — the worker would release its
  channel at dial time, and attempt state would need reconciliation for calls whose
  webhook never arrives.
- **All `failed` treated as transient.** The mock has no reason codes, so invalid
  numbers get retried like transient errors. Real systems need carrier reason codes, a
  DNC/consent check before each dial, and a circuit breaker for provider-wide faults.
- **No crash recovery.** A worker dying mid-call leaves an `in_flight` row forever.
  Needs a lease expiry and a reaper.
- **No calling-hours governance.** A retry can currently be scheduled at 2am.
- **`max_attempts` is a single ceiling on attempt number**, checked against the rule
  for the disposition that just returned, rather than independent per-reason counters.
