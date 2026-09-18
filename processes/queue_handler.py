"""Module to hande queue population"""

import asyncio
import json
import logging
from datetime import date

from automation_server_client import Workqueue

from helpers import config
from processes.process_item import ACTIONS

logger = logging.getLogger(__name__)


def retrieve_items_for_queue(action: str = "nightly_run") -> list[dict]:
    """Build the workqueue: one item, for the action being run.

    Defaults to "nightly_run", the whole pipeline. The seven steps form a
    dependency chain — addresses before people, people before their addresses,
    status before deriving a school from a bevilling, everything before the
    walking distance — and separate workqueue items would give no ordering
    guarantee, so the chain is one item rather than seven.

    Any individual step can be queued instead, for a re-run or a test:

        python main.py --queue --process --action usp_upsert_elev_from_stg

    Validated against process_item.ACTIONS rather than a list kept here, so a
    step added to the dispatch is queueable without touching this file, and a
    typo fails at queue time rather than when the item is picked up.

    Args:
        action:
            Which action to queue. Must be a key of process_item.ACTIONS.

    Returns:
        A single item, referenced {today}_{action} — so queueing the same
        action twice in one day is deduplicated by the caller.

    Raises:
        ValueError: If the action is not one this process knows.
    """

    if action not in ACTIONS:
        raise ValueError(
            f"Unknown action: {action}. Known actions: {', '.join(ACTIONS)}"
        )

    today = date.today().isoformat()

    return [
        {
            "reference": f"{today}_{action}",
            "data": {"date": today, "action": action},
        },
    ]


def create_sort_key(item: dict) -> str:
    """
    Create a sort key based on the entire JSON structure.
    Converts the item to a sorted JSON string for consistent ordering.
    """
    return json.dumps(item, sort_keys=True, ensure_ascii=False)


async def concurrent_add(workqueue: Workqueue, items: list[dict]) -> None:
    """
    Populate the workqueue with items to be processed.
    Uses concurrency and retries with exponential backoff.

    Args:
        workqueue (Workqueue): The workqueue to populate.
        items (list[dict]): List of items to add to the queue.

    Returns:
        None

    Raises:
        Exception: If adding an item fails after all retries.
    """
    sem = asyncio.Semaphore(config.MAX_CONCURRENCY)

    async def add_one(it: dict):
        reference = str(it.get("reference") or "")
        data = {"item": it}

        async with sem:
            for attempt in range(1, config.MAX_RETRIES + 1):
                try:
                    await asyncio.to_thread(workqueue.add_item, data, reference)
                    logger.info("Added item to queue with reference: %s", reference)
                    return True

                except Exception as e:
                    if attempt >= config.MAX_RETRIES:
                        logger.error(
                            "Failed to add item %s after %d attempts: %s",
                            reference,
                            attempt,
                            e,
                        )
                        return False

                    backoff = config.RETRY_BASE_DELAY * (2 ** (attempt - 1))

                    logger.warning(
                        "Error adding %s (attempt %d/%d). Retrying in %.2fs... %s",
                        reference,
                        attempt,
                        config.MAX_RETRIES,
                        backoff,
                        e,
                    )
                    await asyncio.sleep(backoff)

    if not items:
        logger.info("No new items to add.")
        return

    sorted_items = sorted(items, key=create_sort_key)
    logger.info(
        "Processing %d items sorted by complete JSON structure", len(sorted_items)
    )

    results = await asyncio.gather(*(add_one(i) for i in sorted_items))
    successes = sum(1 for r in results if r)
    failures = len(results) - successes

    logger.info(
        "Summary: %d succeeded, %d failed out of %d", successes, failures, len(results)
    )
