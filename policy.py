
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
