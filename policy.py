"""
Retry policy.

Expressed as a data table rather than scattered if/else branches, so the policy can
be read, tuned, and argued about in one place.

THE SPLIT, AND WHY:

  answered   -> terminal, success. Goal achieved.

  voicemail  -> terminal, NOT retried. The call *connected* and a message was
                delivered. Redialling someone whose voicemail already took the
                message doesn't improve the outcome, and in BFSI repeat dialling
                after a connect is a compliance concern, not just an annoyance.
                The correct handling is a hand-off to another channel (SMS /
                WhatsApp), which this system records as an outcome so the
                downstream channel can pick it up.

  no_answer  -> retried. Nothing was delivered and nobody was reached. Trying
                again later, ideally at a different hour of the day, is the
                entire reason a campaign has retries.

  failed     -> retried. Treated as a transient provider-side error.
                CAVEAT: the mock returns `failed` with no reason code, so every
                failure is treated as transient. A real system must split
                permanent failures (invalid number, revoked consent, carrier
                reject) from transient ones -- retrying a dead number burns
                budget and, if consent was revoked, is a violation. Named as a
                production gap.

  busy       -> retried, but on a SHORTER backoff with a TIGHTER budget.
                Busy is positive signal: the line is live and the person is near
                their phone right now. That earns a faster retry than no_answer.
                But it earns a small budget, because rapidly redialling an
                engaged line is exactly the pattern regulators object to.

BACKOFF: exponential with jitter.

    delay = min(base * 2**(attempt_no - 1), cap) * uniform(1 - j, 1 + j)

  Exponential, because a provider returning errors may be degraded and deserves
  increasing room. Capped, so a late attempt doesn't get scheduled hours out.
  Jittered, because that is the part that actually matters operationally: without
  jitter, every contact that fails inside the same window retries inside the same
  window, producing a synchronised burst against the channel cap at precisely the
  moment the provider is least able to absorb it.

SIMPLIFICATION WORTH STATING OUT LOUD: `max_attempts` is a ceiling on the total
attempt number, checked against the rule for the disposition that just came back.
So a contact that goes no_answer -> busy is judged against busy's tighter budget
on that second outcome. A real system would track per-reason counters
independently (e.g. "3 no-answers OR 2 busies, whichever first"). Left simple
here on purpose; the shape of the fix is a counter dict on the contact rather
than a single attempt_no.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from enum import Enum

from provider import Disposition


class Action(str, Enum):
    RETRY = "retry"
    RESOLVED = "resolved"


class Resolution(str, Enum):
    """Why a contact will never be dialled again."""

    ANSWERED = "answered"
    TERMINAL_DISPOSITION = "terminal_disposition"  # voicemail
    EXHAUSTED = "exhausted"  # ran out of retry budget


@dataclass(frozen=True)
class Rule:
    retryable: bool
    max_attempts: int = 1  # ceiling on total attempts for this contact
    base_delay: float = 1.0  # seconds
    cap_delay: float = 8.0  # seconds
    jitter: float = 0.5  # +/- fraction


RULES: dict[Disposition, Rule] = {
    Disposition.ANSWERED: Rule(retryable=False),
    Disposition.VOICEMAIL: Rule(retryable=False),
    Disposition.NO_ANSWER: Rule(
        retryable=True, max_attempts=4, base_delay=1.0, cap_delay=8.0
    ),
    Disposition.FAILED: Rule(
        retryable=True, max_attempts=4, base_delay=1.0, cap_delay=8.0
    ),
    Disposition.BUSY: Rule(
        retryable=True, max_attempts=2, base_delay=0.5, cap_delay=2.0
    ),
}


@dataclass(frozen=True)
class Decision:
    action: Action
    delay: float = 0.0  # seconds until the next attempt (RETRY only)
    resolution: Resolution | None = None  # why it stopped (RESOLVED only)


def backoff_delay(rule: Rule, attempt_no: int, rng: random.Random | None = None) -> float:
    """Exponential backoff with jitter. `attempt_no` is the attempt that just finished."""
    rng = rng or random
    raw = rule.base_delay * (2 ** (attempt_no - 1))
    capped = min(raw, rule.cap_delay)
    return capped * rng.uniform(1.0 - rule.jitter, 1.0 + rule.jitter)


def decide(
    disposition: Disposition,
    attempt_no: int,
    rng: random.Random | None = None,
) -> Decision:
    """
    Given the disposition of the attempt that just completed, decide what happens
    to this contact next.

    Pure function of (disposition, attempt_no) -- no I/O, no state. That makes the
    policy trivially unit-testable in isolation, which is the main reason it lives
    in its own module.
    """
    rule = RULES[disposition]

    if not rule.retryable:
        resolution = (
            Resolution.ANSWERED
            if disposition == Disposition.ANSWERED
            else Resolution.TERMINAL_DISPOSITION
        )
        return Decision(action=Action.RESOLVED, resolution=resolution)

    if attempt_no >= rule.max_attempts:
        return Decision(action=Action.RESOLVED, resolution=Resolution.EXHAUSTED)

    return Decision(action=Action.RETRY, delay=backoff_delay(rule, attempt_no, rng))
