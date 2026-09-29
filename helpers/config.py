"""Module for general configurations of the process"""

import os

MAX_RETRY = 10

# ----------------------
# Queue population settings
# ----------------------
MAX_CONCURRENCY = 100  # tune based on backend capacity
MAX_RETRIES = 3  # transient failure retries per item
RETRY_BASE_DELAY = 0.5  # seconds (exponential backoff)

# ----------------------
# Dry-run mode
# When True: log everything that would happen, but do not write to the
# database or call any mutating endpoints.  Flip to False before deploying.
# ----------------------
DRY_RUN = False

# ----------------------
# calculate_gaaafstand — test switches
#
# Set from the environment so a trial run needs no code change:
#
#     GAAAFSTAND_LIMIT=5 GAAAFSTAND_VERBOSE=1 \
#         python -u main.py --queue --process --action calculate_gaaafstand
#
# LIMIT caps how many students are measured in one run, and narrows the
# selection to CLEAN ones — an Aktiv bevilling, no genbehandling raised — so a
# trial exercises the normal path rather than whatever happens to sort first.
# The students it leaves out keep kraever_genberegning = 1 and are picked up
# by the next run, so a capped run is not a partial failure.
#
# VERBOSE logs every step for each student: which school, which coordinates,
# what the distance API answered, and what was written.  Unusable at full
# volume, which is why it is off by default and why LIMIT exists.
# ----------------------
GAAAFSTAND_LIMIT = int(os.getenv("GAAAFSTAND_LIMIT", "0")) or None
GAAAFSTAND_VERBOSE = os.getenv("GAAAFSTAND_VERBOSE", "").strip().lower() in (
    "1",
    "true",
    "yes",
    "ja",
)
