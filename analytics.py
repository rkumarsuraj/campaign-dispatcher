from __future__ import annotations

from dataclasses import dataclass, field

from store import Store

# ------------------------------- 1. connection rate -------------------------------

SQL_CONNECTION_RATE = """
SELECT
    (SELECT COUNT(DISTINCT contact_id)
       FROM attempts
      WHERE campaign_id = ? AND disposition = 'answered')      AS answered_contacts,
    (SELECT COUNT(DISTINCT contact_id)
       FROM attempts
      WHERE campaign_id = ?)                                   AS dispatched_contacts
"""

# ---------------------------- 2. disposition breakdown ----------------------------

SQL_DISPOSITION_BREAKDOWN = """
SELECT disposition,
       COUNT(*) AS attempts,
       ROUND(100.0 * COUNT(*) / (
           SELECT COUNT(*) FROM attempts
            WHERE campaign_id = ? AND status = 'completed'
       ), 2) AS pct_of_attempts
  FROM attempts
 WHERE campaign_id = ? AND status = 'completed'
 GROUP BY disposition
 ORDER BY attempts DESC
"""

# ------------------------- 3. average attempts to resolution -------------------------

SQL_AVG_ATTEMPTS_ALL_RESOLVED = """
SELECT ROUND(AVG(final_attempt), 3) AS avg_attempts,
       COUNT(*)                     AS contacts
  FROM contact_outcomes
 WHERE campaign_id = ?
"""

SQL_AVG_ATTEMPTS_STRICT = """
SELECT ROUND(AVG(final_attempt), 3) AS avg_attempts,
       COUNT(*)                     AS contacts
  FROM contact_outcomes
 WHERE campaign_id = ?
   AND resolution IN ('answered', 'exhausted')
"""

SQL_ATTEMPTS_BY_RESOLUTION = """
SELECT resolution,
       COUNT(*)                     AS contacts,
       ROUND(AVG(final_attempt), 3) AS avg_attempts
  FROM contact_outcomes
 WHERE campaign_id = ?
 GROUP BY resolution
 ORDER BY contacts DESC
"""

# ------------------------- 4. average time-to-first-connect -------------------------

SQL_TIME_TO_CONNECT = """
WITH first_dispatch AS (
    SELECT contact_id, MIN(dispatched_at_ms) AS started_ms
      FROM attempts
     WHERE campaign_id = ?
     GROUP BY contact_id
),
connected AS (
    SELECT contact_id, MIN(completed_at_ms) AS connected_ms
      FROM attempts
     WHERE campaign_id = ? AND disposition = 'answered'
     GROUP BY contact_id
)
SELECT ROUND(AVG(c.connected_ms - f.started_ms) / 1000.0, 3) AS avg_seconds,
       ROUND(MIN(c.connected_ms - f.started_ms) / 1000.0, 3)  AS min_seconds,
       ROUND(MAX(c.connected_ms - f.started_ms) / 1000.0, 3)  AS max_seconds,
       COUNT(*)                                               AS contacts
  FROM connected c
  JOIN first_dispatch f ON f.contact_id = c.contact_id
"""

# Contrast metric: dial time only, excluding backoff waits.
SQL_DIAL_TIME_TO_CONNECT = """
WITH answered AS (
    SELECT contact_id, attempt_no
      FROM attempts
     WHERE campaign_id = ? AND disposition = 'answered'
)
SELECT ROUND(AVG(total_dial_ms) / 1000.0, 3) AS avg_seconds
  FROM (
    SELECT a.contact_id, SUM(at.latency_ms) AS total_dial_ms
      FROM answered a
      JOIN attempts at
        ON at.campaign_id = ?
       AND at.contact_id = a.contact_id
       AND at.attempt_no <= a.attempt_no
     GROUP BY a.contact_id
  )
"""


@dataclass
class Report:
    campaign_id: str
    answered_contacts: int = 0
    dispatched_contacts: int = 0
    connection_rate_pct: float = 0.0
    total_attempts: int = 0
    dispositions: list[dict] = field(default_factory=list)
    avg_attempts_all_resolved: float | None = None
    resolved_contacts: int = 0
    avg_attempts_strict: float | None = None
    strict_contacts: int = 0
    by_resolution: list[dict] = field(default_factory=list)
    avg_time_to_connect_s: float | None = None
    min_time_to_connect_s: float | None = None
    max_time_to_connect_s: float | None = None
    avg_dial_time_to_connect_s: float | None = None


def build_report(store: Store, campaign_id: str) -> Report:
    report = Report(campaign_id=campaign_id)

    row = store.query(SQL_CONNECTION_RATE, (campaign_id, campaign_id))[0]
    report.answered_contacts = row["answered_contacts"]
    report.dispatched_contacts = row["dispatched_contacts"]
    if report.dispatched_contacts:
        report.connection_rate_pct = round(
            100.0 * report.answered_contacts / report.dispatched_contacts, 2
        )

    breakdown = store.query(SQL_DISPOSITION_BREAKDOWN, (campaign_id, campaign_id))
    report.dispositions = [dict(r) for r in breakdown]
    report.total_attempts = sum(r["attempts"] for r in breakdown)

    row = store.query(SQL_AVG_ATTEMPTS_ALL_RESOLVED, (campaign_id,))[0]
    report.avg_attempts_all_resolved = row["avg_attempts"]
    report.resolved_contacts = row["contacts"]

    row = store.query(SQL_AVG_ATTEMPTS_STRICT, (campaign_id,))[0]
    report.avg_attempts_strict = row["avg_attempts"]
    report.strict_contacts = row["contacts"]

    report.by_resolution = [
        dict(r) for r in store.query(SQL_ATTEMPTS_BY_RESOLUTION, (campaign_id,))
    ]

    row = store.query(SQL_TIME_TO_CONNECT, (campaign_id, campaign_id))[0]
    report.avg_time_to_connect_s = row["avg_seconds"]
    report.min_time_to_connect_s = row["min_seconds"]
    report.max_time_to_connect_s = row["max_seconds"]

    row = store.query(SQL_DIAL_TIME_TO_CONNECT, (campaign_id, campaign_id))[0]
    report.avg_dial_time_to_connect_s = row["avg_seconds"]

    return report


def _bar(pct: float, width: int = 28) -> str:
    filled = int(round(pct / 100 * width))
    return "#" * filled + "." * (width - filled)


def print_report(report: Report, extra: dict | None = None) -> None:
    line = "=" * 64
    print(f"\n{line}\nCAMPAIGN ANALYTICS -- {report.campaign_id}\n{line}")

    print("\n1. OVERALL CONNECTION RATE")
    print(
        f"   {report.connection_rate_pct}%  "
        f"({report.answered_contacts} answered / "
        f"{report.dispatched_contacts} contacts dispatched)"
    )

    print(f"\n2. DISPOSITION BREAKDOWN  (per completed attempt, n={report.total_attempts})")
    print(f"   {'disposition':<12} {'count':>7} {'pct':>8}")
    print(f"   {'-' * 12} {'-' * 7} {'-' * 8}")
    for row in report.dispositions:
        print(
            f"   {row['disposition']:<12} {row['attempts']:>7} "
            f"{row['pct_of_attempts']:>7}%  {_bar(row['pct_of_attempts'])}"
        )

    print("\n3. AVERAGE ATTEMPTS BEFORE RESOLUTION")
    print(
        f"   all resolved contacts      : {report.avg_attempts_all_resolved} "
        f"(n={report.resolved_contacts})"
    )
    print(
        f"   answered or exhausted only : {report.avg_attempts_strict} "
        f"(n={report.strict_contacts})   <- as literally worded"
    )
    print(f"\n   {'resolution':<22} {'contacts':>9} {'avg attempts':>13}")
    print(f"   {'-' * 22} {'-' * 9} {'-' * 13}")
    for row in report.by_resolution:
        print(f"   {row['resolution']:<22} {row['contacts']:>9} {row['avg_attempts']:>13}")

    print("\n4. AVERAGE TIME-TO-FIRST-CONNECT  (answered contacts only)")
    print(
        f"   wall-clock (incl. backoff) : {report.avg_time_to_connect_s}s"
        f"   [min {report.min_time_to_connect_s}s / max {report.max_time_to_connect_s}s]"
    )
    print(
        f"   dial time only (excl. wait): {report.avg_dial_time_to_connect_s}s"
        "   <- provider speed, not campaign speed"
    )

    if extra:
        print("\nRUN DIAGNOSTICS")
        for key, value in extra.items():
            print(f"   {key:<28}: {value}")
    print(f"{line}\n")


def export_csv(store: Store, campaign_id: str, path: str) -> None:
    """Dump the raw attempts table so the numbers above can be checked by hand."""
    import csv

    rows = store.query(
        """
        SELECT contact_id, attempt_no, status, disposition,
               dispatched_at, completed_at, latency_ms
          FROM attempts
         WHERE campaign_id = ?
         ORDER BY contact_id, attempt_no
        """,
        (campaign_id,),
    )
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "contact_id",
                "attempt_no",
                "status",
                "disposition",
                "dispatched_at",
                "completed_at",
                "latency_ms",
            ]
        )
        for row in rows:
            writer.writerow(list(row))
