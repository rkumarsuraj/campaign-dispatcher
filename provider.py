"""
Mock telephony provider.

Stands in for a real provider (Twilio / Exotel / Plivo). Simulates network + ring
time with a random delay, then returns a disposition drawn from a weighted
distribution.

DISPOSITION DISTRIBUTION (documented, as the assignment asks):

    answered    35%   picked up by a human
    no_answer   30%   rang out, nobody picked up
    voicemail   15%   answering machine / voicemail detected (AMD)
    busy        12%   line engaged
    failed       8%   provider-side error placing the call

These weights are shaped to look like a plausible BFSI outbound campaign: answered
is the single largest bucket but still a minority, no_answer is close behind (the
dominant reason a campaign needs retries at all), and provider-side `failed` is
deliberately rare, since a provider failing 1-in-12 calls would be a broken provider.

CALL LATENCY: uniform 0.1-2.0s, as specified. Real dial latency isn't uniform --
answered calls take longer than a busy signal, which returns almost instantly --
so `LATENCY_BY_DISPOSITION` applies a per-disposition range instead. Set
`REALISTIC_LATENCY = False` to fall back to a flat uniform(0.1, 2.0).
"""

from __future__ import annotations

import asyncio
import random
from enum import Enum


class Disposition(str, Enum):
    """Outcome of a single call attempt."""

    ANSWERED = "answered"
    NO_ANSWER = "no_answer"
    VOICEMAIL = "voicemail"
    BUSY = "busy"
    FAILED = "failed"


# Weighted distribution -- must sum to 100.
DISPOSITION_WEIGHTS: dict[Disposition, int] = {
    Disposition.ANSWERED: 35,
    Disposition.NO_ANSWER: 30,
    Disposition.VOICEMAIL: 15,
    Disposition.BUSY: 12,
    Disposition.FAILED: 8,
}

assert sum(DISPOSITION_WEIGHTS.values()) == 100, "weights must sum to 100"

REALISTIC_LATENCY = True

# (min_seconds, max_seconds) per disposition.
LATENCY_BY_DISPOSITION: dict[Disposition, tuple[float, float]] = {
    # Full ring cycle then a human picks up -- the slowest outcome.
    Disposition.ANSWERED: (0.8, 2.0),
    # Rings until the provider gives up.
    Disposition.NO_ANSWER: (1.2, 2.0),
    # Rings, then AMD kicks in.
    Disposition.VOICEMAIL: (0.9, 1.8),
    # Engaged tone comes back from the carrier almost immediately.
    Disposition.BUSY: (0.1, 0.4),
    # Provider rejects before dialling.
    Disposition.FAILED: (0.1, 0.5),
}

FLAT_LATENCY = (0.1, 2.0)


def _pick_disposition(rng: random.Random) -> Disposition:
    dispositions = list(DISPOSITION_WEIGHTS.keys())
    weights = list(DISPOSITION_WEIGHTS.values())
    return rng.choices(dispositions, weights=weights, k=1)[0]


def _pick_latency(disposition: Disposition, rng: random.Random) -> float:
    lo, hi = (
        LATENCY_BY_DISPOSITION[disposition] if REALISTIC_LATENCY else FLAT_LATENCY
    )
    return rng.uniform(lo, hi)


async def dispatch_call(
    phone_number: str,
    rng: random.Random | None = None,
) -> Disposition:
    """
    Simulate placing one outbound call.

    Blocks (asynchronously) for a simulated call duration, then returns the
    disposition. `await asyncio.sleep` is load-bearing here: a blocking
    `time.sleep` would freeze the event loop and serialise the whole campaign,
    destroying the concurrency the dispatcher exists to provide.

    NOTE ON THE REAL WORLD: a real provider does not work this way. It would
    accept the dial request, return a call SID immediately, and report the final
    disposition asynchronously via webhook. That inversion is called out as a
    production gap in the write-up -- it changes the shape of the dispatcher,
    because a worker would release its channel at dial time rather than holding
    it until the outcome is known.
    """
    rng = rng or random
    disposition = _pick_disposition(rng)
    await asyncio.sleep(_pick_latency(disposition, rng))
    return disposition
