"""Campaign configuration. Persisted with the campaign row for reproducibility."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class CampaignConfig:
    """
    All knobs in one place.

    `max_concurrency` is the number of worker coroutines, and therefore the hard
    ceiling on calls in flight. It represents a provider's concurrent-channel
    limit.
    """

    max_concurrency: int = 20
    cohort_size: int = 300
    random_seed: int | None = 42

    # Scheduler tick ceiling. The scheduler normally sleeps until the next retry
    # is due; this caps how long it will sleep with an empty heap before waking
    # to re-check. Purely a liveness guard.
    scheduler_max_sleep: float = 0.5

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)
