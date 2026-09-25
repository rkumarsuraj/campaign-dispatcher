"""
Driver: runs a simulated campaign against a synthetic cohort and prints analytics.

Usage:
    python run_campaign.py
    python run_campaign.py --contacts 500 --concurrency 30
    python run_campaign.py --contacts 300 --concurrency 20 --export attempts.csv

THE IDEMPOTENCY DEMO
Worth explaining why it is here. In a clean single-process design, duplicate
dispatches never arise naturally -- the driver enqueues each contact exactly once,
so the dedupe path would never execute and the guarantee could only be *asserted*,
never shown. So the driver deliberately submits one contact's attempt-1 job twice,
concurrently, and then verifies afterwards that exactly one attempt row exists and
the dedupe counter fired. Disable with --no-dupe-demo.

Note that the duplicate is submitted as a JOB, not as a contact: the
unresolved-contact counter is set from the cohort size via register_contacts(), so
a duplicate job cannot inflate it and cannot stall termination.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
import time
import uuid

from analytics import build_report, export_csv, print_report
from config import CampaignConfig
from dispatcher import Dispatcher, Job
from store import Contact, Store

FIRST_NAMES = [
    "Aarav", "Diya", "Kabir", "Ananya", "Vivaan", "Ishita", "Arjun", "Meera",
    "Rohan", "Saanvi", "Aditya", "Nithya", "Kiran", "Priya", "Rahul", "Tanvi",
]
LAST_NAMES = [
    "Sharma", "Reddy", "Iyer", "Nair", "Patel", "Menon", "Rao", "Gupta",
    "Krishnan", "Desai", "Bose", "Chawla",
]
PRODUCTS = ["personal_loan", "credit_card", "gold_loan", "insurance_renewal"]


def build_cohort(size: int, rng: random.Random) -> list[Contact]:
    """Synthetic cohort. Arbitrary variables (name, account_ref) as the spec allows."""
    contacts = []
    for i in range(size):
        name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
        contacts.append(
            Contact(
                contact_id=f"C{i:05d}",
                # Reserved 999-prefix test range; not dialable even by accident.
                phone_number=f"+9199{rng.randint(10_000_000, 99_999_999)}",
                name=name,
                account_ref=f"{rng.choice(PRODUCTS)}:{rng.randint(100000, 999999)}",
            )
        )
    return contacts


async def run(args: argparse.Namespace) -> None:
    config = CampaignConfig(
        max_concurrency=args.concurrency,
        cohort_size=args.contacts,
        random_seed=args.seed,
    )
    rng = random.Random(config.random_seed)
    campaign_id = f"camp-{uuid.uuid4().hex[:8]}"

    store = Store(args.db)
    store.create_campaign(campaign_id, config.to_json())

    cohort = build_cohort(config.cohort_size, rng)
    store.insert_contacts(cohort)

    dispatcher = Dispatcher(campaign_id, store, config, rng)
    dispatcher.register_contacts(len(cohort))

    print(
        f"\ncampaign      : {campaign_id}"
        f"\ncohort        : {len(cohort)} contacts"
        f"\nconcurrency   : {config.max_concurrency} channels"
        f"\ndatabase      : {args.db}"
    )

    # Every contact starts at attempt 1. The attempt number is stamped on the job
    # here, at creation -- never computed later inside a worker.
    for contact in cohort:
        await dispatcher.submit(
            Job(
                campaign_id=campaign_id,
                contact_id=contact.contact_id,
                phone_number=contact.phone_number,
                attempt_no=1,
            )
        )

    # --- idempotency demo: submit one contact's attempt 1 a second time ---
    dupe_target = cohort[0].contact_id if args.dupe_demo else None
    if dupe_target:
        await dispatcher.submit(
            Job(
                campaign_id=campaign_id,
                contact_id=cohort[0].contact_id,
                phone_number=cohort[0].phone_number,
                attempt_no=1,
            )
        )
        print(f"dupe demo     : submitted {dupe_target} attempt 1 twice")

    print("\ndispatching...\n")
    started = time.perf_counter()
    stats = await dispatcher.run()
    elapsed = time.perf_counter() - started

    # --- verify the idempotency claim ---
    dupe_result = "not run"
    if dupe_target:
        rows = store.query(
            "SELECT COUNT(*) AS n FROM attempts"
            " WHERE campaign_id = ? AND contact_id = ? AND attempt_no = 1",
            (campaign_id, dupe_target),
        )
        n = rows[0]["n"]
        dupe_result = (
            f"PASS -- {n} row for {dupe_target} attempt 1, {stats.deduped} deduped"
            if n == 1 and stats.deduped >= 1
            else f"FAIL -- {n} rows, {stats.deduped} deduped"
        )

    report = build_report(store, campaign_id)
    print_report(
        report,
        extra={
            "wall-clock runtime": f"{elapsed:.2f}s",
            "calls actually placed": stats.dispatched,
            "duplicate dispatches blocked": stats.deduped,
            "retries scheduled": stats.retries_scheduled,
            "contacts resolved": stats.resolved,
            "effective throughput": f"{stats.dispatched / elapsed:.1f} calls/s",
            "idempotency check": dupe_result,
        },
    )

    if args.export:
        export_csv(store, campaign_id, args.export)
        print(f"raw attempts exported to {args.export}\n")

    store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Outbound voice campaign dispatcher")
    parser.add_argument("--contacts", type=int, default=300, help="cohort size")
    parser.add_argument(
        "--concurrency", type=int, default=20, help="max calls in flight (channels)"
    )
    parser.add_argument("--seed", type=int, default=42, help="RNG seed; 0 for random")
    parser.add_argument("--db", default="campaign.db", help="SQLite path")
    parser.add_argument("--export", help="write raw attempts to this CSV path")
    parser.add_argument(
        "--no-dupe-demo",
        dest="dupe_demo",
        action="store_false",
        help="skip the duplicate-dispatch demonstration",
    )
    parser.add_argument("--verbose", action="store_true")
    parser.set_defaults(dupe_demo=True)
    args = parser.parse_args()

    if args.seed == 0:
        args.seed = None

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
