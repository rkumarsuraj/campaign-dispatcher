from __future__ import annotations

import asyncio
import heapq
import itertools
import logging
import random
import time
from dataclasses import dataclass, field

from config import CampaignConfig
from policy import Action, decide
from provider import Disposition, dispatch_call
from store import Contact, Store

log = logging.getLogger("dispatcher")


@dataclass(frozen=True)
class Job:
    """
    One unit of work: dial this contact, as this attempt number.

    `attempt_no` is part of the job's identity, assigned at creation. Together with
    campaign_id and contact_id it forms the idempotency key.
    """

    campaign_id: str
    contact_id: str
    phone_number: str
    attempt_no: int


@dataclass
class Stats:
    dispatched: int = 0
    deduped: int = 0
    retries_scheduled: int = 0
    resolved: int = 0
    dispositions: dict[str, int] = field(default_factory=dict)


class Dispatcher:
    def __init__(
        self,
        campaign_id: str,
        store: Store,
        config: CampaignConfig,
        rng: random.Random | None = None,
    ) -> None:
        self.campaign_id = campaign_id
        self.store = store
        self.config = config
        self.rng = rng or random.Random()

        self.queue: asyncio.Queue[Job] = asyncio.Queue()

        # Min-heap of (due_at_monotonic, tiebreaker, Job). The tiebreaker keeps
        # heapq from ever having to compare two Jobs, which are not orderable.
        self._retry_heap: list[tuple[float, int, Job]] = []
        self._heap_seq = itertools.count()
        self._wakeup = asyncio.Event()

        self._unresolved = 0
        self._done = asyncio.Event()

        self.stats = Stats()
        self._tasks: list[asyncio.Task] = []

    # ------------------------------ submission ------------------------------

    def register_contacts(self, count: int) -> None:
        """
        Tell the dispatcher how many contacts it is responsible for resolving.

        Called once, before the run. Deliberately separate from job submission:
        submitting a duplicate job must NOT inflate this count, which is what
        makes the idempotency demo in the driver safe.
        """
        self._unresolved = count

    async def submit(self, job: Job) -> None:
        """
        Public entry point. Put a job on the ready queue.

        This is intentionally callable more than once with the same job -- that is
        the duplicate-dispatch case the idempotency mechanism exists to handle.
        """
        await self.queue.put(job)

    # -------------------------------- run --------------------------------

    async def run(self) -> Stats:
        """Start the worker pool and scheduler, wait for the campaign to finish."""
        if self._unresolved == 0:
            return self.stats

        self._tasks = [
            asyncio.create_task(self._worker(i), name=f"worker-{i}")
            for i in range(self.config.max_concurrency)
        ]
        self._tasks.append(asyncio.create_task(self._scheduler(), name="scheduler"))

        await self._done.wait()

        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        return self.stats

    # ------------------------------- worker -------------------------------

    async def _worker(self, worker_id: int) -> None:
        while True:
            job = await self.queue.get()
            try:
                await self._process(job)
            except Exception:
                # A worker must never die silently -- that would leak one of the N
                # channels for the rest of the campaign. Log and carry on.
                log.exception(
                    "worker %s failed on %s attempt %s",
                    worker_id,
                    job.contact_id,
                    job.attempt_no,
                )
            finally:
                self.queue.task_done()

    async def _process(self, job: Job) -> None:
        # --- THE IDEMPOTENCY GATE ---
        # Claim the attempt in the database BEFORE dialling. If the claim fails,
        # another worker already owns this exact (campaign, contact, attempt) and
        # this worker must not place a call.
        claimed = await self.store.claim_attempt(
            job.campaign_id, job.contact_id, job.attempt_no
        )
        if not claimed:
            self.stats.deduped += 1
            log.debug(
                "deduped duplicate dispatch: %s attempt %s",
                job.contact_id,
                job.attempt_no,
            )
            return

        # --- dial ---
        started = time.perf_counter()
        disposition: Disposition = await dispatch_call(job.phone_number, self.rng)
        latency_ms = int((time.perf_counter() - started) * 1000)

        self.stats.dispatched += 1
        self.stats.dispositions[disposition.value] = (
            self.stats.dispositions.get(disposition.value, 0) + 1
        )

        await self.store.record_attempt(
            job.campaign_id,
            job.contact_id,
            job.attempt_no,
            disposition,
            latency_ms,
        )

        # --- decide what happens next ---
        decision = decide(disposition, job.attempt_no, self.rng)

        if decision.action is Action.RETRY:
            next_job = Job(
                campaign_id=job.campaign_id,
                contact_id=job.contact_id,
                phone_number=job.phone_number,
                attempt_no=job.attempt_no + 1,  # assigned HERE, not in the worker
            )
            self._schedule_retry(next_job, decision.delay)
            self.stats.retries_scheduled += 1
        else:
            await self.store.record_outcome(
                job.campaign_id,
                job.contact_id,
                decision.resolution,
                job.attempt_no,
            )
            self._resolve_contact()

    # ------------------------- retries & scheduler -------------------------

    def _schedule_retry(self, job: Job, delay: float) -> None:
        due_at = time.monotonic() + delay
        heapq.heappush(self._retry_heap, (due_at, next(self._heap_seq), job))
        self._wakeup.set()

    async def _scheduler(self) -> None:
        """
        Move due retries from the heap onto the ready queue.

        Sleeps until the earliest deadline rather than polling on a fixed tick, so
        it costs nothing while idle.
        """
        while True:
            now = time.monotonic()

            while self._retry_heap and self._retry_heap[0][0] <= now:
                _, _, job = heapq.heappop(self._retry_heap)
                await self.queue.put(job)

            if self._retry_heap:
                timeout = max(0.0, self._retry_heap[0][0] - time.monotonic())
                timeout = min(timeout, self.config.scheduler_max_sleep)
            else:
                timeout = self.config.scheduler_max_sleep

            self._wakeup.clear()
            try:
                await asyncio.wait_for(self._wakeup.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                pass

    # ------------------------------ termination ------------------------------

    def _resolve_contact(self) -> None:
        self.stats.resolved += 1
        self._unresolved -= 1
        if self._unresolved <= 0:
            self._done.set()
