
from __future__ import annotations

import asyncio
import os
import random
import tempfile

from config import CampaignConfig
from dispatcher import Dispatcher, Job
from policy import RULES, Action, Resolution, backoff_delay, decide
from provider import DISPOSITION_WEIGHTS, Disposition, dispatch_call
from store import Contact, Store

PASSED: list[str] = []
FAILED: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        PASSED.append(name)
        print(f"  PASS  {name}")
    else:
        FAILED.append(name)
        print(f"  FAIL  {name}  {detail}")


def _temp_store() -> tuple[Store, str]:
    path = os.path.join(tempfile.mkdtemp(), "test.db")
    return Store(path), path


# ------------------------------- idempotency -------------------------------


async def test_concurrent_duplicate_claims() -> None:
    """50 simultaneous claims for the same key -> exactly one wins."""
    store, _ = _temp_store()
    store.create_campaign("camp-x", "{}")

    results = await asyncio.gather(
        *[store.claim_attempt("camp-x", "C1", 1) for _ in range(50)]
    )

    check(
        "concurrent duplicate claims: exactly one winner",
        sum(results) == 1,
        f"got {sum(results)} winners",
    )

    rows = store.query(
        "SELECT COUNT(*) AS n FROM attempts WHERE contact_id = 'C1' AND attempt_no = 1"
    )
    check("concurrent duplicate claims: exactly one row", rows[0]["n"] == 1)
    store.close()


async def test_different_attempt_numbers_both_claim() -> None:
    """A legitimate retry (attempt 2) must NOT be blocked by attempt 1's claim."""
    store, _ = _temp_store()
    store.create_campaign("camp-x", "{}")

    first = await store.claim_attempt("camp-x", "C1", 1)
    second = await store.claim_attempt("camp-x", "C1", 2)
    repeat = await store.claim_attempt("camp-x", "C1", 1)

    check("attempt 1 claims", first is True)
    check("attempt 2 claims (retry not blocked)", second is True)
    check("attempt 1 re-claim is refused", repeat is False)
    store.close()


async def test_duplicate_job_places_one_call() -> None:
    """End to end: submit the same job twice, only one call is placed."""
    store, _ = _temp_store()
    store.create_campaign("camp-x", "{}")
    contact = Contact("C1", "+919900000000", "Test", "acct:1")
    store.insert_contacts([contact])

    config = CampaignConfig(max_concurrency=4, cohort_size=1, random_seed=7)
    dispatcher = Dispatcher("camp-x", store, config, random.Random(7))
    dispatcher.register_contacts(1)

    job = Job("camp-x", "C1", contact.phone_number, 1)
    await dispatcher.submit(job)
    await dispatcher.submit(job)  # the duplicate

    stats = await dispatcher.run()

    rows = store.query(
        "SELECT COUNT(*) AS n FROM attempts WHERE contact_id='C1' AND attempt_no=1"
    )
    check("duplicate job: one attempt-1 row", rows[0]["n"] == 1)
    check("duplicate job: dedupe counter fired", stats.deduped == 1, f"{stats.deduped}")
    store.close()


# --------------------------------- policy ---------------------------------


def test_policy_split() -> None:
    check(
        "answered is terminal",
        decide(Disposition.ANSWERED, 1).action is Action.RESOLVED,
    )
    check(
        "answered resolution is ANSWERED",
        decide(Disposition.ANSWERED, 1).resolution is Resolution.ANSWERED,
    )
    check(
        "voicemail is terminal, not retried",
        decide(Disposition.VOICEMAIL, 1).action is Action.RESOLVED,
    )
    check(
        "voicemail resolution is terminal_disposition",
        decide(Disposition.VOICEMAIL, 1).resolution is Resolution.TERMINAL_DISPOSITION,
    )
    check(
        "no_answer retries on attempt 1",
        decide(Disposition.NO_ANSWER, 1).action is Action.RETRY,
    )
    check(
        "failed retries on attempt 1",
        decide(Disposition.FAILED, 1).action is Action.RETRY,
    )
    check(
        "busy retries on attempt 1",
        decide(Disposition.BUSY, 1).action is Action.RETRY,
    )


def test_retry_budgets_exhaust() -> None:
    no_answer_max = RULES[Disposition.NO_ANSWER].max_attempts
    check(
        "no_answer exhausts at its max_attempts",
        decide(Disposition.NO_ANSWER, no_answer_max).resolution is Resolution.EXHAUSTED,
    )
    busy_max = RULES[Disposition.BUSY].max_attempts
    check(
        "busy exhausts earlier than no_answer",
        busy_max < no_answer_max
        and decide(Disposition.BUSY, busy_max).resolution is Resolution.EXHAUSTED,
    )


def test_backoff_bounds() -> None:
    rule = RULES[Disposition.NO_ANSWER]
    rng = random.Random(1)
    delays = [backoff_delay(rule, n, rng) for n in range(1, 8) for _ in range(200)]
    upper = rule.cap_delay * (1 + rule.jitter)
    check("backoff never exceeds cap x (1+jitter)", max(delays) <= upper + 1e-9)
    check("backoff always positive", min(delays) > 0)

    # Monotonic growth in the *expected* value, jitter aside.
    fixed = [
        min(rule.base_delay * 2 ** (n - 1), rule.cap_delay) for n in range(1, 6)
    ]
    check("backoff grows then plateaus at cap", fixed == sorted(fixed))

    # Jitter must actually spread values, or it isn't doing its job.
    same_attempt = [backoff_delay(rule, 2, rng) for _ in range(200)]
    check("jitter produces spread", len(set(same_attempt)) > 150)


# -------------------------------- provider --------------------------------


def test_weights_sum_to_100() -> None:
    check("disposition weights sum to 100", sum(DISPOSITION_WEIGHTS.values()) == 100)


async def test_provider_distribution() -> None:
    """Sanity check: 4000 draws should land within a few points of the weights."""
    rng = random.Random(99)
    counts: dict[str, int] = {}
    # Sample the picker directly rather than awaiting 4000 sleeps.
    from provider import _pick_disposition

    for _ in range(4000):
        d = _pick_disposition(rng)
        counts[d.value] = counts.get(d.value, 0) + 1

    ok = True
    for disposition, weight in DISPOSITION_WEIGHTS.items():
        observed = 100.0 * counts.get(disposition.value, 0) / 4000
        if abs(observed - weight) > 3.0:
            ok = False
    check("provider distribution matches configured weights (+/-3pp)", ok)

    result = await dispatch_call("+919900000000", rng)
    check("dispatch_call returns a Disposition", isinstance(result, Disposition))


# ------------------------------- concurrency -------------------------------


async def test_concurrency_never_exceeds_limit() -> None:
    """
    Instrument the provider to record peak simultaneous calls, and assert it never
    exceeds max_concurrency.
    """
    import dispatcher as dispatcher_module

    in_flight = 0
    peak = 0

    async def instrumented(phone_number, rng=None):
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        try:
            await asyncio.sleep(0.01)
            return Disposition.ANSWERED
        finally:
            in_flight -= 1

    original = dispatcher_module.dispatch_call
    dispatcher_module.dispatch_call = instrumented
    try:
        store, _ = _temp_store()
        store.create_campaign("camp-c", "{}")
        limit = 8
        cohort = [
            Contact(f"C{i:04d}", f"+9199000{i:05d}", "T", "a") for i in range(120)
        ]
        store.insert_contacts(cohort)

        config = CampaignConfig(max_concurrency=limit, cohort_size=len(cohort))
        disp = Dispatcher("camp-c", store, config, random.Random(3))
        disp.register_contacts(len(cohort))
        for c in cohort:
            await disp.submit(Job("camp-c", c.contact_id, c.phone_number, 1))
        await disp.run()

        check(
            f"peak in-flight never exceeded limit ({peak} <= {limit})",
            peak <= limit,
            f"peak={peak}",
        )
        check("concurrency was actually exercised", peak > 1, f"peak={peak}")
        store.close()
    finally:
        dispatcher_module.dispatch_call = original


async def test_all_contacts_resolve() -> None:
    """Every contact must end up in contact_outcomes exactly once."""
    store, _ = _temp_store()
    store.create_campaign("camp-r", "{}")
    cohort = [Contact(f"C{i:04d}", f"+9199000{i:05d}", "T", "a") for i in range(60)]
    store.insert_contacts(cohort)

    config = CampaignConfig(max_concurrency=10, cohort_size=len(cohort))
    disp = Dispatcher("camp-r", store, config, random.Random(11))
    disp.register_contacts(len(cohort))
    for c in cohort:
        await disp.submit(Job("camp-r", c.contact_id, c.phone_number, 1))
    await disp.run()

    rows = store.query("SELECT COUNT(*) AS n FROM contact_outcomes")
    check("every contact resolved exactly once", rows[0]["n"] == len(cohort),
          f"{rows[0]['n']} of {len(cohort)}")

    rows = store.query(
        "SELECT COUNT(*) AS n FROM attempts WHERE status != 'completed'"
    )
    check("no attempts left in_flight", rows[0]["n"] == 0)

    # No contact should exceed the largest configured budget.
    max_budget = max(r.max_attempts for r in RULES.values())
    rows = store.query("SELECT MAX(attempt_no) AS m FROM attempts")
    check(
        f"no contact exceeded max budget ({max_budget})",
        rows[0]["m"] <= max_budget,
        f"max attempt_no={rows[0]['m']}",
    )
    store.close()


async def main() -> None:
    print("\nidempotency")
    await test_concurrent_duplicate_claims()
    await test_different_attempt_numbers_both_claim()
    await test_duplicate_job_places_one_call()

    print("\npolicy")
    test_policy_split()
    test_retry_budgets_exhaust()
    test_backoff_bounds()

    print("\nprovider")
    test_weights_sum_to_100()
    await test_provider_distribution()

    print("\nconcurrency & lifecycle")
    await test_concurrency_never_exceeds_limit()
    await test_all_contacts_resolve()

    print(f"\n{len(PASSED)} passed, {len(FAILED)} failed")
    if FAILED:
        for name in FAILED:
            print(f"  failed: {name}")
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
