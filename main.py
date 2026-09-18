"""
This is the main entry point for the process
"""

import argparse
import asyncio
import logging
import sys

from automation_server_client import AutomationServer, Workqueue
from mbu_rpa_core.exceptions import BusinessError, ProcessError
from mbu_rpa_core.process_states import CompletedState

from helpers import ats_functions, config
from processes.application_handler import close, reset, startup
from processes.error_handling import ErrorContext, handle_error
from processes.finalize_process import finalize_process
from processes.process_item import ACTIONS, process_item
from processes.queue_handler import concurrent_add, retrieve_items_for_queue

logger = logging.getLogger(__name__)


# ╔══════════════════════════════════════════════╗
# ║ 🔥 REMOVE BEFORE DEPLOYMENT (TEMP OVERRIDES) 🔥 ║
# ╚══════════════════════════════════════════════╝
# import requests
# import urllib3
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# _old_request = requests.Session.request
# def unsafe_request(self, *args, **kwargs):
#     kwargs['verify'] = False
#     return _old_request(self, *args, **kwargs)
# requests.Session.request = unsafe_request
# ╔══════════════════════════════════════════════╗
# ║ 🔥 REMOVE BEFORE DEPLOYMENT (TEMP OVERRIDES) 🔥 ║
# ╚══════════════════════════════════════════════╝


async def populate_queue(workqueue: Workqueue, action: str):
    """Populate the workqueue with items to be processed.

    Args:
        workqueue:
            The ATS workqueue to add to.

        action:
            Which action to queue — see processes.process_item.ACTIONS.
    """

    logger.info("Populating workqueue with action: %s", action)

    items_to_queue = retrieve_items_for_queue(action)

    queue_references = {str(r) for r in ats_functions.get_workqueue_items(workqueue)}

    new_items: list[dict] = []
    for item in items_to_queue:
        reference = str(item.get("reference") or "")
        if reference and reference in queue_references:
            logger.info(
                "Reference: %s already in queue. Item: %s not added",
                reference,
                item,
            )
        else:
            new_items.append(item)

    await concurrent_add(workqueue, new_items)
    logger.info("Finished populating workqueue.")


async def process_workqueue(workqueue: Workqueue):
    """Process items from the workqueue."""

    logger.info("Processing workqueue...")

    startup()

    error_count = 0

    while error_count < config.MAX_RETRY:
        for item in workqueue:
            try:
                with item:
                    data, reference = ats_functions.get_item_info(item)

                    try:
                        logger.info("Processing item with reference: %s", reference)
                        process_item(data, reference)

                        completed_state = CompletedState.completed(
                            "Process completed without exceptions"
                        )
                        item.complete(str(completed_state))

                        continue

                    except BusinessError as e:
                        context = ErrorContext(
                            item=item,
                            action=item.pending_user(str(e)),
                            send_mail=False,
                            process_name=workqueue.name,
                        )
                        handle_error(
                            error=e,
                            log=logger.info,
                            context=context,
                        )

                    except Exception as e:
                        pe = ProcessError(str(e))
                        raise pe from e

            except ProcessError as e:
                context = ErrorContext(
                    item=item,
                    action=item.fail,
                    send_mail=True,
                    process_name=workqueue.name,
                )
                handle_error(
                    error=e,
                    log=logger.error,
                    context=context,
                )
                error_count += 1
                reset()

        break

    logger.info("Finished processing workqueue.")
    close()


async def finalize(workqueue: Workqueue):
    """Finalize process."""

    logger.info("Finalizing process...")

    try:
        finalize_process()
        logger.info("Finished finalizing process.")

    except BusinessError as e:
        handle_error(error=e, log=logger.info)

    except Exception as e:
        pe = ProcessError(str(e))
        context = ErrorContext(
            send_mail=True,
            process_name=workqueue.name,
        )
        handle_error(error=pe, log=logger.error, context=context)

        raise pe from e


def parse_args() -> argparse.Namespace:
    """Parse the phase flags and the action to queue.

    The phases are independent and always run in the order queue -> process ->
    finalize, whatever order they are given on the command line.

    --action only affects --queue. Processing takes whatever is in the queue,
    so running --process alone picks up whatever a previous --queue put there.
    """

    parser = argparse.ArgumentParser(
        description="Nightly befordring data run.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python main.py --queue --process\n"
            "      the full nightly pipeline\n\n"
            "  python main.py --queue --process --action usp_upsert_elev_from_stg\n"
            "      re-run one step by hand\n"
        ),
    )

    parser.add_argument("--queue", action="store_true", help="populate the workqueue")
    parser.add_argument("--process", action="store_true", help="process the workqueue")
    parser.add_argument("--finalize", action="store_true", help="run finalisation")

    parser.add_argument(
        "--action",
        default="nightly_run",
        choices=list(ACTIONS),
        metavar="ACTION",
        help=(
            "which action to queue (default: nightly_run, the whole pipeline). "
            "One of: " + ", ".join(ACTIONS)
        ),
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    ats_functions.init_logger()

    ats = AutomationServer.from_environment()

    prod_workqueue = ats.workqueue()
    process = ats.process

    if args.queue:
        asyncio.run(populate_queue(prod_workqueue, args.action))

    if args.process:
        asyncio.run(process_workqueue(prod_workqueue))

    if args.finalize:
        asyncio.run(finalize(prod_workqueue))

    sys.exit(0)
