"""Module to handle item processing"""

import logging
import os
import uuid
from functools import partial

import pyodbc
import requests
from mbu_rpa_core.exceptions import BusinessError

from helpers import config

logger = logging.getLogger(__name__)


# The two databases this process touches, read once.
#
#   SERVER29     LOIS. The source for both the address register and the
#                person -> address links. Read only, and a different server,
#                which is the whole reason the CPR list has to be shipped to it
#                rather than joined against.
#
#   BEFORDRING   Befordringssystemet. Everything this process writes goes here,
#                and every stored procedure it calls lives here.
#
# One target, deliberately. This used to be two: a SQLAlchemy session factory
# reading DBCONNECTIONSTRINGBEFORDRING alongside raw pyodbc steps reading
# another variable. The run could then stage rows in one database and merge
# them in another — which is exactly how it failed, with "Could not find stored
# procedure 'befordring.usp_upsert_elev_from_stg'" on a procedure that had been
# deployed, just not to the database that step connected to.
CONN_STRING_SERVER29 = os.getenv("DBCONNECTIONSTRINGSERVER29")
CONN_STRING_BEFORDRING = os.getenv("DBCONNECTIONSTRINGBEFORDRING")


def process_item(item_data: dict, item_reference: str):
    """Dispatch processing based on the action in item_data.

    Looked up in ACTIONS, defined at the bottom of this module once the
    functions it names exist. That table is also what --action validates
    against and what the queue is built from, so the three cannot drift apart.
    """

    assert item_data, "Item data is required"
    assert item_reference, "Item reference is required"

    action = item_data.get("action")
    handler = ACTIONS.get(action)

    if handler is None:
        raise BusinessError(
            f"Unknown action: {action}. Known actions: {', '.join(ACTIONS)}"
        )

    handler()


def _connect(autocommit: bool = True):
    """Open a connection to Befordringssystemet.

    autocommit defaults to True because most statements here are EXEC calls,
    and every procedure in this pipeline opens and commits its own transaction.
    Wrapping those in an outer one only nests them, and a procedure that hits
    its CATCH rolls back every level at once — leaving the caller holding a
    transaction that no longer exists.

    Pass autocommit=False where several statements must land together.
    """

    _require(DBCONNECTIONSTRINGBEFORDRING=CONN_STRING_BEFORDRING)

    return pyodbc.connect(CONN_STRING_BEFORDRING, autocommit=autocommit)


def _rows_as_dicts(cursor) -> list[dict]:
    """Read a result set as dicts, using the cursor's own column names."""

    if cursor.description is None:
        return []

    columns = [column[0] for column in cursor.description]

    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _require(**named: str | None) -> None:
    """Fail with the variable's own name when a required value is unset.

    Checked when a step runs rather than at import: a --queue run needs none of
    these, and failing at import would break it for no reason.
    """

    missing = [name for name, value in named.items() if not value]

    if missing:
        raise ValueError(f"Missing environment variable(s): {', '.join(missing)}")


def _run_sp(procedure: str, label: str) -> dict | None:
    """Execute one of the upsert procedures and log the counts it returns.

    Every procedure in this pipeline ends with a single-row result set of
    counts — staged, updated, inserted, skipped. They are the only visibility
    into what a nightly run actually did, so they are logged rather than
    discarded.

    Not dry-run aware: these procedures take no @dry_run parameter, unlike
    usp_recalculate_bevilling_status. With config.DRY_RUN set they are skipped
    entirely, because there is no way to ask them to look without touching.
    """

    if config.DRY_RUN:
        logger.info("DRY RUN — %s: skipped (procedure has no dry-run mode)", label)
        return None

    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(f"EXEC [befordring].[{procedure}]")
        rows = _rows_as_dicts(cursor)

    counts = rows[0] if rows else {}

    logger.info(
        "%s: %s",
        label,
        ", ".join(f"{k}={v}" for k, v in counts.items()) or "no counts returned",
    )

    return counts


def _nightly_run():
    """The whole nightly pipeline, in dependency order.

    One workqueue item rather than seven, because the order is not a
    preference — each step depends on the one before, and separate items give
    no ordering guarantee.

    The order, and why it is this order:

      1. Addresses first. Everything downstream writes an adresse_id, and
         Elev's and Foraelder's foreign keys to Adresse are trusted
         (migration 021), so an address that has not been imported yet is a
         write that fails rather than one that passes quietly.

      2. Elev, then 3. Foraelder. Foraelder has a foreign key to Elev, so a
         guardian whose child is new this year needs that child to exist first.

      4. Person addresses. Needs the rows from 2 and 3 to exist to match
         against, and the addresses from 1 to point at.

      5. Status recalculation. Needs current skolekode (step 2) and adresse_id
         (step 4) to judge mismatches, and this is where Kommende becomes
         Aktiv, Aktiv becomes Udløbet, and genbehandling/revurdering are
         raised.

      6. Derive the student's school from their bevilling. AFTER step 5 on
         purpose: step 5 decides which bevilling is the active one, and that is
         the first thing this looks at. Run it before, and a newly-activated
         bevilling's school does not reach the student until tomorrow night.

      7. Walking distance, last: it needs the school from step 6 and the
         address from step 4, and it is the only step that clears
         kraever_genberegning — the flag steps 2, 4 and 6 raise.

    No step is wrapped in an outer transaction. Each procedure is atomic in
    itself, and a failure part-way leaves the earlier steps applied: re-running
    is safe, because every step is an upsert keyed on the natural identifier.
    """

    logger.info("nightly_run: starting")

    logger.info("nightly_run: 1/7 — addresses from LOIS")
    _fetch_and_upsert_addresses()

    logger.info("nightly_run: 2/7 — Elev_STG -> Elev")
    _run_sp("usp_upsert_elev_from_stg", "upsert_elev")

    logger.info("nightly_run: 3/7 — Foraelder_STG -> Foraelder")
    _run_sp("usp_upsert_foraelder_from_stg", "upsert_foraelder")

    logger.info("nightly_run: 4/7 — adresse_id for elever and forældre")
    _fetch_and_upsert_person_adresser()

    logger.info("nightly_run: 5/7 — recalculate bevilling status")
    _exec_sp()

    logger.info("nightly_run: 6/7 — derive school from bevilling")
    _run_sp("usp_sync_elev_matrikel_from_bevilling", "sync_elev_matrikel")

    logger.info("nightly_run: 7/7 — walking distance")
    _calculate_gaaafstand()

    logger.info("nightly_run: complete")


def _exec_sp():
    """Run usp_recalculate_bevilling_status for all bevillinger (no bevilling_id).

    Skolekode- and adresse-mismatch detection is built into the SP, so a single
    nightly call covers both status recalculation and data-difference checks.

    In dry-run mode the SP is called with @dry_run = 1, which returns the
    same result set but does not commit any changes to the database.

    This writes nothing itself. It used to post-process Kommende->Aktiv
    transitions by copying the newly-active bevilling's school onto Elev and
    re-running the SP, to stop the transition raising a flag. That is gone, for
    three reasons:

      * It never worked. It wrote Elev.matrikel_id, but the SP compares
        Elev.skolekode against the skolekode on the bevilling's matrikel — a
        separate denormalised column the RPA never touched. The "safety net"
        re-run saw the same mismatch and changed nothing.

      * It suppressed the wrong thing. Since revurdering and genbehandling were
        split, a skolekode mismatch raises genbehandling, not revurdering, and
        genbehandling is meant to be seen: it holds until a caseworker marks it
        handled. A Kommende bevilling activating at a school that does not match
        the child's registered school is exactly that case. It is expected when
        a child changes school, and clearing it by hand is cheap.

      * Elev is not ours to write. See _calculate_gaaafstand below: the data
        worker owns Elev and writes the authoritative values there. Copying a
        bevilling's school back onto the child inverts the direction of truth,
        and syncing matrikel_id without skolekode would leave the row
        internally inconsistent — the two columns naming different schools.
    """

    dry_run_flag = 1 if config.DRY_RUN else 0

    if config.DRY_RUN:
        logger.info("DRY RUN — exec_sp: calling SP with @dry_run = 1 (no writes)")

    sql = """
        EXEC [befordring].[usp_recalculate_bevilling_status]
            @bevilling_id = ?,
            @today        = ?,
            @dry_run      = ?
    """

    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, None, None, dry_run_flag)
        rows = _rows_as_dicts(cursor)

    changed = [r for r in rows if r.get("status_will_change")]
    logger.info(
        "exec_sp: %d bevillinger evaluated, %d would change status",
        len(rows),
        len(changed),
    )
    for r in changed:
        logger.info(
            "  bevilling_id=%s  %s → %s  reason=%s",
            r.get("bevilling_id"),
            r.get("current_status_text"),
            r.get("calculated_status_text"),
            r.get("status_reason"),
        )


def _fetch_and_upsert_addresses():
    """
    Fetches addresses from LOIS on Server 29,
    inserts them into Adresse_STG on DEV,
    and then calls the stored procedure that upserts into Adresse.
    """

    _require(
        DBCONNECTIONSTRINGSERVER29=CONN_STRING_SERVER29,
        DBCONNECTIONSTRINGBEFORDRING=CONN_STRING_BEFORDRING,
    )

    load_id = str(uuid.uuid4())

    fetch_sql = """
        SELECT
            CONVERT(NVARCHAR(36), [AdresseId]) AS adresse_id,
            [AdresseBetegnelse] AS adresse_tekst,
            CAST([Lat] AS FLOAT) AS latitude,
            CAST([Long] AS FLOAT) AS longitude
        FROM [LOIS].[DAR].[AdresseDkGeoView];
    """

    insert_stage_sql = """
        INSERT INTO [Befordringssystemet].[befordring].[Adresse_STG]
        (
            load_id,
            adresse_id,
            adresse_tekst,
            latitude,
            longitude
        )
        VALUES
        (
            ?,
            ?,
            ?,
            ?,
            ?
        );
    """

    upsert_sql = """
        EXEC [Befordringssystemet].[befordring].[usp_upsert_adresser_from_stg]
            @load_id = ?,
            @clear_stage_afterwards = 1;
    """

    batch_size = 10_000
    total_staged_rows = 0

    result = None

    print(f"Starting address import with load_id: {load_id}")
    print()

    with pyodbc.connect(CONN_STRING_SERVER29) as source_conn, pyodbc.connect(CONN_STRING_BEFORDRING) as target_conn:
        source_cursor = source_conn.cursor()
        target_cursor = target_conn.cursor()

        target_cursor.fast_executemany = True

        source_cursor.execute(fetch_sql)

        while True:
            rows = source_cursor.fetchmany(batch_size)

            if not rows:
                break

            stage_rows = [
                (
                    load_id,
                    row.adresse_id,
                    row.adresse_tekst,
                    row.latitude,
                    row.longitude,
                )
                for row in rows
            ]

            target_cursor.executemany(insert_stage_sql, stage_rows)
            target_conn.commit()

            total_staged_rows += len(stage_rows)

            print(f"Staged rows so far: {total_staged_rows}")

        print()
        print("Finished staging rows.")
        print("Starting upsert...")
        print()

        target_cursor.execute(upsert_sql, load_id)

        result = target_cursor.fetchone()

        target_conn.commit()

    print("Address import completed.")
    print()

    if result:
        print(f"Deduplicated staged rows: {result.staged_rows}")
        print(f"Updated rows: {result.updated_rows}")
        print(f"Inserted rows: {result.inserted_rows}")

    print()
    print(f"Total raw staged rows inserted: {total_staged_rows}")

    return {
        "load_id": load_id,
        "total_staged_rows": total_staged_rows,
        "staged_rows": result.staged_rows if result else None,
        "updated_rows": result.updated_rows if result else None,
        "inserted_rows": result.inserted_rows if result else None,
    }


def _fetch_and_upsert_person_adresser():
    """Resolve adresse_id for every elev and forælder from LOIS.CPR.PersonGeoView.

    Elev_STG and Foraelder_STG never carry an address — the data worker's
    source does not have one. The link between a person and an address lives in
    LOIS on server 29 instead, keyed on PNR_0 (ten digits, no dash, which is
    the format Elev.cpr and Foraelder.cpr_foraelder use).

    LOIS and Befordringssystemet are on separate servers, so there is no join to
    make. The CPRs we need are read from the target first, then sent back to
    server 29 as a filter. Chunked, because SQL Server caps a statement at 2100
    parameters — and because the alternative, pulling the whole view and
    filtering in Python, means dragging every citizen in the municipality
    across the wire to resolve a few thousand.

    Rows land in Elev_Adresse_STG under a per-run load_id, and
    usp_upsert_adresse_ids_from_stg drains them into Elev.adresse_id and
    Foraelder.adresse_id. That procedure also skips any adresse_id that Adresse
    does not know about, so this must run AFTER the address import.

    Parter are deliberately not resolved: their addresses are entered by hand
    in the application, not imported.
    """

    _require(
        DBCONNECTIONSTRINGSERVER29=CONN_STRING_SERVER29,
        DBCONNECTIONSTRINGBEFORDRING=CONN_STRING_BEFORDRING,
    )

    load_id = str(uuid.uuid4())

    # Both sides of the case in one list: a forælder is looked up exactly like
    # a student, and Elev_Adresse_STG's column is `cpr`, not `cpr_elev`.
    cpr_sql = """
        SELECT cpr           AS cpr FROM [befordring].[Elev]
        UNION
        SELECT cpr_foraelder      FROM [befordring].[Foraelder]
    """

    insert_stage_sql = """
        INSERT INTO [befordring].[Elev_Adresse_STG] (load_id, cpr, adresse_id)
        VALUES (?, ?, ?)
    """

    upsert_sql = "{CALL [befordring].[usp_upsert_adresse_ids_from_stg] (?)}"

    # Well under the 2100-parameter cap, and few enough round trips that the
    # chunking is not the slow part.
    chunk_size = 900
    batch_size = 1000

    total_staged_rows = 0
    result = None

    print(f"Starting person-address import with load_id: {load_id}")
    print()

    with pyodbc.connect(CONN_STRING_SERVER29) as source_conn, pyodbc.connect(CONN_STRING_BEFORDRING) as target_conn:
        source_cursor = source_conn.cursor()
        target_cursor = target_conn.cursor()

        target_cursor.fast_executemany = True

        target_cursor.execute(cpr_sql)

        cprs = [
            row.cpr.strip()
            for row in target_cursor.fetchall()
            if row.cpr and row.cpr.strip()
        ]

        print(f"Resolving addresses for {len(cprs)} people")
        print()

        for offset in range(0, len(cprs), chunk_size):
            chunk = cprs[offset:offset + chunk_size]

            placeholders = ",".join("?" for _ in chunk)

            fetch_sql = f"""
                SELECT
                    [PNR_0]      AS cpr,
                    CONVERT(NVARCHAR(36), [AdresseId]) AS adresse_id
                FROM [LOIS].[CPR].[PersonGeoView]
                WHERE [PNR_0] IN ({placeholders})
                AND   [AdresseId] IS NOT NULL
            """

            source_cursor.execute(fetch_sql, chunk)

            while True:
                rows = source_cursor.fetchmany(batch_size)

                if not rows:
                    break

                stage_rows = [(load_id, row.cpr, row.adresse_id) for row in rows]

                target_cursor.executemany(insert_stage_sql, stage_rows)
                target_conn.commit()

                total_staged_rows += len(stage_rows)

            print(f"Staged rows so far: {total_staged_rows}")

        print()
        print("Finished staging rows.")
        print("Starting upsert...")
        print()

        target_cursor.execute(upsert_sql, load_id)

        result = target_cursor.fetchone()

        target_conn.commit()

    print("Person-address import completed.")
    print()

    if result:
        print(f"Deduplicated staged rows: {result.staged_rows}")
        print(f"Skipped (address unknown): {result.skipped_unknown_adresse}")
        print(f"Elev updated: {result.elev_updated}")
        print(f"Foraelder updated: {result.foraelder_updated}")

    print()
    print(f"Total raw staged rows inserted: {total_staged_rows}")

    return {
        "load_id": load_id,
        "requested_cprs": len(cprs),
        "total_staged_rows": total_staged_rows,
        "staged_rows": result.staged_rows if result else None,
        "skipped_unknown_adresse": result.skipped_unknown_adresse if result else None,
        "elev_updated": result.elev_updated if result else None,
        "foraelder_updated": result.foraelder_updated if result else None,
    }


def _calculate_gaaafstand():
    """Recalculate walking distance (skoleafstand) for students flagged by the
    data worker's nightly job.

    The data worker sets kraever_genberegning = 1 on Elev whenever it detects
    a change in adresse_id, skolekode, or matrikel_id.  It also writes the
    authoritative current values for those fields directly onto Elev, so we
    do NOT sync anything from Bevilling — Elev's own data is the truth here.

    Steps:
      1. SELECT students with kraever_genberegning = 1, pulling home address
         coordinates from Adresse and school coordinates from Skolematrikel or
         Ungdomsuddannelse (whichever is set on Elev).
      2. For each, call the backend walking-distance endpoint.
      3. Write the returned distance to Elev.skoleafstand and reset
         kraever_genberegning = 0 in a single UPDATE per student.
    """

    api_base = os.getenv("API_ENDPOINT", "").rstrip("/")
    api_key  = os.getenv("API_KEY", "")
    headers  = {"X-API-Key": api_key}

    # -----------------------------------------------------------------------
    # 1. Find all students the data worker has flagged for recalculation.
    #    School coordinates come from Elev's own matrikel_id /
    #    ungdomsuddannelse_id — NOT from the bevilling.
    # -----------------------------------------------------------------------
    select_sql = """
        SELECT
            e.cpr,
            ad.latitude                              AS addr_lat,
            ad.longitude                             AS addr_lon,
            COALESCE(sm.latitude,  uu.latitude)      AS school_lat,
            COALESCE(sm.longitude, uu.longitude)     AS school_lon
        FROM      [befordring].[Elev]              e
        JOIN      [befordring].[Adresse]            ad
                  ON  ad.adresse_id = e.adresse_id
        LEFT JOIN [befordring].[Skolematrikel]      sm
                  ON  sm.matrikel_id = e.matrikel_id
        LEFT JOIN [befordring].[Ungdomsuddannelse]  uu
                  ON  uu.ungdomsuddannelse_id = e.ungdomsuddannelse_id
        WHERE e.kraever_genberegning = 1
    """

    # -----------------------------------------------------------------------
    # 3. Write calculated distance and clear the flag in one statement.
    # -----------------------------------------------------------------------
    update_sql = """
        UPDATE [befordring].[Elev]
        SET    skoleafstand         = ?,
               kraever_genberegning = 0
        WHERE  cpr = ?
    """

    if config.DRY_RUN:
        logger.info("DRY RUN — calculate_gaaafstand: will log candidates and distances but write nothing")

    with _connect(autocommit=False) as conn:
        cursor = conn.cursor()
        cursor.execute(select_sql)
        candidates = _rows_as_dicts(cursor)

        logger.info("calculate_gaaafstand: %d students flagged for distance recalculation", len(candidates))

        if not candidates:
            return

        # -------------------------------------------------------------------
        # 2. Call the walking-distance API for each candidate
        # -------------------------------------------------------------------
        updated = 0
        skipped = 0

        for row in candidates:
            school_lat = row["school_lat"]
            school_lon = row["school_lon"]

            if school_lat is None or school_lon is None:
                logger.warning(
                    "calculate_gaaafstand: no school coordinates for CPR %s "
                    "(matrikel_id / ungdomsuddannelse_id may be NULL on Elev) — skipping",
                    row["cpr"],
                )
                skipped += 1
                continue

            try:
                resp = requests.get(
                    f"{api_base}/bevilling/calculate_walking_distance",
                    params={
                        "lat1": row["addr_lat"],
                        "lon1": row["addr_lon"],
                        "lat2": school_lat,
                        "lon2": school_lon,
                    },
                    headers=headers,
                    timeout=15,
                )
                resp.raise_for_status()
                distance_km = resp.json()["distance_km"]
            except Exception as exc:
                logger.warning(
                    "calculate_gaaafstand: distance API failed for CPR %s — %s",
                    row["cpr"],
                    exc,
                )
                skipped += 1
                continue

            if config.DRY_RUN:
                logger.info(
                    "DRY RUN — CPR %s: would set skoleafstand = %.3f km, kraever_genberegning → 0",
                    row["cpr"],
                    distance_km,
                )
            else:
                cursor.execute(update_sql, distance_km, row["cpr"])
            updated += 1

        if not config.DRY_RUN:
            conn.commit()

    action = "would write" if config.DRY_RUN else "wrote"
    logger.info(
        "calculate_gaaafstand: %s %d distances, %d skipped (no school coords or API error)",
        action,
        updated,
        skipped,
    )


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------
# Every action that can be queued, in the order the nightly run performs them.
# "nightly_run" is the whole chain; the rest are its individual steps, exposed
# so one can be re-run by hand without waiting for a night:
#
#     python main.py --queue --process --action usp_upsert_elev_from_stg
#
# Running a step on its own is safe — each is an upsert keyed on a natural
# identifier, so repeating it changes nothing the second time. Running one OUT
# of order is not necessarily safe; see _nightly_run for what depends on what.
#
# Insertion order is the run order, and --action lists these as its choices, so
# `--help` doubles as the pipeline documentation.
ACTIONS = {
    "nightly_run": _nightly_run,

    "_fetch_and_upsert_addresses": _fetch_and_upsert_addresses,
    "usp_upsert_elev_from_stg": partial(
        _run_sp, "usp_upsert_elev_from_stg", "upsert_elev"
    ),
    "usp_upsert_foraelder_from_stg": partial(
        _run_sp, "usp_upsert_foraelder_from_stg", "upsert_foraelder"
    ),
    "_fetch_and_upsert_person_adresser": _fetch_and_upsert_person_adresser,
    "exec_sp": _exec_sp,
    "usp_sync_elev_matrikel_from_bevilling": partial(
        _run_sp, "usp_sync_elev_matrikel_from_bevilling", "sync_elev_matrikel"
    ),
    "_calculate_gaaafstand": _calculate_gaaafstand,
}
