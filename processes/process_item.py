"""Module to handle item processing"""

import logging
import os
import re
import time
import uuid
import xml.etree.ElementTree as ET
from functools import partial

import pyodbc
import requests
from mbu_dev_shared_components.database.connection import RPAConnection
from mbu_rpa_core.exceptions import BusinessError
from requests_ntlm import HttpNtlmAuth

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

    logger.info("nightly_run: 1/8 — addresses from LOIS")
    _fetch_and_upsert_addresses()

    logger.info("nightly_run: 2/8 — Elev_STG -> Elev")
    _run_sp("usp_upsert_elev_from_stg", "upsert_elev")

    logger.info("nightly_run: 3/8 — Foraelder_STG -> Foraelder")
    _run_sp("usp_upsert_foraelder_from_stg", "upsert_foraelder")

    logger.info("nightly_run: 4/8 — adresse_id for elever and forældre")
    _fetch_and_upsert_person_adresser()

    logger.info("nightly_run: 5/8 — recalculate bevilling status")
    _exec_sp()

    logger.info("nightly_run: 6/8 — derive school from bevilling")
    _run_sp("usp_sync_elev_matrikel_from_bevilling", "sync_elev_matrikel")

    logger.info("nightly_run: 7/8 — walking distance")
    _calculate_gaaafstand()

    logger.info("nightly_run: 8/8 — GO case links")
    _embed_esdh_url()

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


# How many measurements to commit at a time. Small enough that Elev is never
# locked long enough for the application to notice, large enough that the
# round-trips do not dominate. Nothing depends on the exact value.
_GAAAFSTAND_BATCH = 100

# How often to say where the run is, in seconds. The step is one external
# round-trip per student and runs for many minutes, so silence has to be kept
# short enough that a stall is distinguishable from ordinary slowness.
_GAAAFSTAND_HEARTBEAT = 30

# GO case keys look like PPR-2026-123456-001: a base case and a sub-case.
# The link points at the BASE — that is the student's case page — so the
# trailing group is stripped. Anchored and strict on purpose: a key that does
# not have this shape gets no link at all rather than a guessed one, because a
# wrong link into a case system is worse than no link.
_PPR_SAG = re.compile(r"^(PPR-\d{4}-\d+)(?:-\d+)?$", re.IGNORECASE)

# GO answers the metadata call with JSON whose "Metadata" field is an XML row,
# and the relative case path — "cases/PPR01/PPR-2026-123456" — is one of its
# attributes. PPR01 is a per-case system id that exists nowhere in the
# befordring database, which is the whole reason for this step.
_GO_CASE_URL_ATTRIB = "ows_CaseUrl"
_GO_SIDE = "SitePages/Home.aspx"

# Give up after this many failures in a row. One failure is a bad row; this
# many is the service being down, and there is nothing to gain from spending
# hours proving it one student at a time.
_GAAAFSTAND_MAX_I_TRAEK = 25

# A rate-limited request is retried rather than counted as a loss, because the
# student is fine — we simply asked too fast. Waiting a full window is the
# only wait that helps: OpenRouteService counts over a rolling minute, so a
# shorter pause just spends another request on the same rejection.
_GAAAFSTAND_429_FORSOEG = 3
_GAAAFSTAND_429_PAUSE = 65

# How far the throttle is allowed to slow itself down when it keeps being
# rate-limited. 6 seconds is 10 requests a minute — far below any plan, so
# hitting this ceiling means the quota is exhausted, not the pace wrong.
_GAAAFSTAND_MAX_INTERVAL = 6.0

# Case links committed per transaction. Same reasoning as the distance
# batch: short enough that Bevilling is never locked long enough for the
# application to notice.
_ESDH_URL_BATCH = 100


def _varighed(sekunder: float) -> str:
    """Seconds as mm:ss, or h:mm:ss once it runs past an hour."""

    sekunder = int(max(sekunder, 0))
    timer, rest = divmod(sekunder, 3600)
    minutter, sek = divmod(rest, 60)

    return f"{timer}:{minutter:02d}:{sek:02d}" if timer else f"{minutter}:{sek:02d}"


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

    api_base = os.getenv("BEFORDRING_API_ENDPOINT", "").rstrip("/")
    api_key  = os.getenv("BEFORDRING_API_KEY", "")
    headers  = {"X-API-Key": api_key}

    # -----------------------------------------------------------------------
    # 1. Find all students the data worker has flagged for recalculation.
    #    School coordinates come from Elev's own matrikel_id /
    #    ungdomsuddannelse_id — NOT from the bevilling.
    # -----------------------------------------------------------------------
    # How many are flagged but not measurable. Counted rather than listed:
    # with the full student population this is a large number and it is not a
    # problem — it is the normal resting state for a student without a
    # bevilling. Logged so a sudden change in it is visible.
    afventer_sql = """
        SELECT COUNT(*)
        FROM      [befordring].[Elev]              e
        LEFT JOIN [befordring].[Adresse]            ad
                  ON  ad.adresse_id = e.adresse_id
        LEFT JOIN [befordring].[Skolematrikel]      sm
                  ON  sm.matrikel_id = e.matrikel_id
        LEFT JOIN [befordring].[Ungdomsuddannelse]  uu
                  ON  uu.ungdomsuddannelse_id = e.ungdomsuddannelse_id
        WHERE e.kraever_genberegning = 1
          AND (
                  COALESCE(sm.latitude, uu.latitude) IS NULL
               OR ad.latitude IS NULL
              )
    """

    # TOP and the extra columns cost nothing on a full run: without a limit
    # the TOP clause is omitted entirely, and the names are three columns on a
    # query that already joins those tables.
    top = f"TOP ({int(config.GAAAFSTAND_LIMIT)}) " if config.GAAAFSTAND_LIMIT else ""

    # Test runs measure CLEAN students only: an Aktiv bevilling with no
    # genbehandling raised. Without this a capped run would take whatever
    # sorted first, which is as likely to be a student with a school or
    # address mismatch as not — and a trial that trips over a known-bad row
    # proves nothing about the normal path.
    rene_elever = """
          AND EXISTS (
                  SELECT 1
                  FROM   [befordring].[Bevilling] b
                  JOIN   [befordring].[Status]    st ON st.status_id = b.status_id
                  WHERE  b.cpr_elev = e.cpr
                  AND    b.aktiv = 1
                  AND    st.status_tekst = N'Aktiv'
                  AND    ISNULL(b.genbehandling, 0) = 0
              )
    """ if config.GAAAFSTAND_LIMIT else ""

    select_sql = f"""
        SELECT {top}
            e.cpr,
            e.skolekode,
            ad.adresse_tekst                         AS addr_tekst,
            COALESCE(sm.matrikel_navn, uu.ungdomsuddannelse_navn) AS school_navn,
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
          /* Only students who can actually be measured.

             Elev now holds every student in the municipality, not just the
             ones with a bevilling, and usp_upsert_elev_from_stg raises
             kraever_genberegning on every NEW student. Without these two
             conditions the candidate set is most of the table, and each of
             those rows is fetched, warned about and skipped — every night,
             for ever, because the flag is only cleared on a successful
             measurement.

             The flag is deliberately LEFT RAISED on the students excluded
             here. "Distance never computed" is true of them, and the moment
             a bevilling gives them a school,
             usp_sync_elev_matrikel_from_bevilling raises the flag again on
             the change and they appear here of their own accord. Clearing it
             would say the opposite and buy nothing. */
          AND COALESCE(sm.latitude,  uu.latitude)  IS NOT NULL
          AND COALESCE(sm.longitude, uu.longitude) IS NOT NULL
          AND ad.latitude  IS NOT NULL
          AND ad.longitude IS NOT NULL
          {rene_elever}
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

    # -----------------------------------------------------------------------
    # The connection is opened, read from and CLOSED before any HTTP happens.
    #
    # This step used to hold one transaction open across the whole loop:
    # autocommit=False, an UPDATE per student inside the loop, one commit at
    # the end. Every UPDATE takes an exclusive row lock on Elev and holds it
    # until that commit, and past a few thousand locks SQL Server escalates to
    # a lock on the whole table — while the loop is still waiting on
    # OpenRouteService, one student at a time. The application's own queries
    # then queue behind it and the site serves 504s until the run finishes.
    #
    # Nothing here needs a long transaction. The reads are a snapshot, the
    # writes are independent of each other, and the flag is only cleared on a
    # successful measurement — so committing in batches is not just safe, it
    # makes a run that dies half way keep what it had instead of losing all
    # of it.
    # -----------------------------------------------------------------------
    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(select_sql)
        candidates = _rows_as_dicts(cursor)

        cursor.execute(afventer_sql)
        afventer = cursor.fetchone()[0]

    if config.GAAAFSTAND_PER_MINUTE:
        logger.info(
            "calculate_gaaafstand: pacing at %d request(s)/minute to stay "
            "inside the OpenRouteService plan — about %s for %d student(s).",
            config.GAAAFSTAND_PER_MINUTE,
            _varighed(len(candidates) * 60 / config.GAAAFSTAND_PER_MINUTE),
            len(candidates),
        )
    else:
        logger.warning(
            "calculate_gaaafstand: throttle disabled "
            "(GAAAFSTAND_PER_MINUTE=0). Only do this against a routing "
            "service with no rate limit.\n"
        )

    if config.GAAAFSTAND_LIMIT:
        logger.info(
            "calculate_gaaafstand: TEST RUN — capped at %d student(s), and "
            "only ones with an Aktiv bevilling and no genbehandling. The "
            "rest keep kraever_genberegning = 1 for the next run.",
            config.GAAAFSTAND_LIMIT,
        )

    logger.info(
        "calculate_gaaafstand: %d student(s) can be measured. A further "
        "%d are flagged but have no school yet — they keep the flag and "
        "appear here once a bevilling gives them one.",
        len(candidates),
        afventer,
    )

    if not candidates:
        return

    updated = 0
    skipped = 0
    # Failures grouped by what went wrong, not logged one student at a time.
    # When the distance API is down or rate-limiting, every candidate fails
    # the same way, and thousands of identical warnings bury the one line
    # that says how many and why.
    fejl: dict[str, list[str]] = {}
    ventende: list[tuple[float, str]] = []
    i_traek = 0

    def _noter_fejl(grund: str, cpr: str) -> None:
        """Record a failure, and say so out loud the first time it happens.

        Aggregating keeps a bad night from printing thousands of identical
        warnings, but waiting until the end to say ANYTHING means a run where
        the distance API is unreachable looks like a run that is working.
        The first of each kind is announced; the rest are counted.
        """

        if grund not in fejl:
            logger.warning(
                "calculate_gaaafstand: %s (first seen on CPR %s; further "
                "occurrences are counted, not logged)",
                grund,
                cpr,
            )

        fejl.setdefault(grund, []).append(cpr)

    startet = time.monotonic()
    sidste_puls = startet

    # Seconds to leave between requests. The loop is sequential, so pacing is
    # a sleep before each call rather than a token bucket — there is never
    # more than one request in flight to burst with.
    interval = 60 / config.GAAAFSTAND_PER_MINUTE if config.GAAAFSTAND_PER_MINUTE else 0
    naeste_tidligst = 0.0

    def _skriv(batch: list[tuple[float, str]]) -> None:
        """One short transaction per batch — locks held for milliseconds."""

        if not batch:
            return

        if config.DRY_RUN:
            logger.info("DRY RUN — would commit %d measurement(s)", len(batch))
            return

        t0 = time.monotonic()

        with _connect(autocommit=False) as skrive_conn:
            skrive_conn.cursor().executemany(update_sql, batch)
            skrive_conn.commit()

        logger.info(
            "calculate_gaaafstand: committed %d measurement(s) in %.2fs — "
            "%d of %d written so far",
            len(batch),
            time.monotonic() - t0,
            updated,
            len(candidates),
        )

    for i, row in enumerate(candidates, start=1):
        # Say where the run is BEFORE anything that can `continue`. This sat
        # at the bottom of the loop, after two `continue`s — so a run where
        # every call failed printed the opening count and then nothing at
        # all, for hours, which reads exactly like a hang. The one state
        # where progress reporting matters most was the one state that
        # silenced it.
        #
        # On the clock rather than every N students, so the interval stays
        # the same however fast or slow the distance API happens to be.
        naa = time.monotonic()

        if naa - sidste_puls >= _GAAAFSTAND_HEARTBEAT:
            sidste_puls = naa
            forloebet = naa - startet
            tempo = (i - 1) / forloebet if forloebet else 0
            tilbage = (len(candidates) - i + 1) / tempo if tempo else 0

            logger.info(
                "calculate_gaaafstand: %d/%d (%.0f%%) — %d measured, %d "
                "skipped, %.1f/s, %s elapsed, ~%s left",
                i - 1,
                len(candidates),
                100 * (i - 1) / len(candidates),
                updated,
                skipped,
                tempo,
                _varighed(forloebet),
                _varighed(tilbage),
            )

        school_lat = row["school_lat"]
        school_lon = row["school_lon"]

        # A safety net rather than the normal path: the query above now
        # excludes rows without coordinates, so reaching this means the
        # two have drifted apart.
        if school_lat is None or school_lon is None:
            _noter_fejl(
                "no school coordinates even though select_sql required "
                "them — the query and this check have drifted apart",
                row["cpr"],
            )
            skipped += 1
            continue

        if config.GAAAFSTAND_VERBOSE:
            logger.info(
                "  [%d/%d] CPR %s (skolekode %s)\n"
                "        hjem:  %s  (%s, %s)\n"
                "        skole: %s  (%s, %s)",
                i,
                len(candidates),
                row["cpr"],
                row.get("skolekode"),
                row.get("addr_tekst") or "(ingen adressetekst)",
                row["addr_lat"],
                row["addr_lon"],
                row.get("school_navn") or "(intet navn)",
                school_lat,
                school_lon,
            )

        # Pace to the plan's limit. Measured from the START of the previous
        # request, so a slow response counts towards the interval instead of
        # being added to it — otherwise a 2-second call plus a 1-second wait
        # would run at 20/minute, not 60.
        #
        # A 429 is retried, not counted as a loss: nothing is wrong with the
        # student, we just asked too fast. And the pace is PERMANENTLY slowed
        # each time it happens, because the alternative is what the first run
        # did — keep asking at a rate the plan will not serve, so the rolling
        # window never drains and every remaining request is rejected. Forty
        # students went through, then nothing, for the rest of the run.
        distance_km = None
        grund = None

        for forsoeg in range(1, _GAAAFSTAND_429_FORSOEG + 1):
            if interval:
                vent = naeste_tidligst - time.monotonic()

                if vent > 0:
                    time.sleep(vent)

            naeste_tidligst = time.monotonic() + interval

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

                # The backend turns EVERY OpenRouteService failure into a 502
                # with the real reason in the body:
                #
                #     raise HTTPException(502, detail=f"Distance API error: {e}")
                #
                # raise_for_status() reports only "502 Server Error: Bad
                # Gateway", the same sentence whether ORS rate-limited us,
                # rejected the key or could not route between two points. The
                # body is the only place the answer exists.
                if resp.ok:
                    svar = resp.json()
                    distance_km = svar["distance_km"]

                    if config.GAAAFSTAND_VERBOSE:
                        logger.info(
                            "        svar:  HTTP %s — %.3f km, %s min",
                            resp.status_code,
                            distance_km,
                            svar.get("duration_minutes", "?"),
                        )

                    break

                besked = f"HTTP {resp.status_code} — {resp.text[:300].strip()}"
                rate_limited = resp.status_code == 429 or "429" in resp.text
            except Exception as exc:
                besked = str(exc)
                rate_limited = False

            grund = f"distance API failed: {besked}"

            if not rate_limited or forsoeg == _GAAAFSTAND_429_FORSOEG:
                break

            if interval:
                interval = min(interval * 1.5, _GAAAFSTAND_MAX_INTERVAL)

            logger.warning(
                "calculate_gaaafstand: rate limited by OpenRouteService on "
                "attempt %d for CPR %s. Waiting %ds for the window to drain "
                "and slowing to %.0f request(s)/minute for the rest of the "
                "run.",
                forsoeg,
                row["cpr"],
                _GAAAFSTAND_429_PAUSE,
                60 / interval if interval else 0,
            )

            time.sleep(_GAAAFSTAND_429_PAUSE)
            naeste_tidligst = time.monotonic()

        if distance_km is None:
            _noter_fejl(grund or "distance API failed: unknown", row["cpr"])
            skipped += 1
            i_traek += 1

            # Nothing is getting through. At a 15-second timeout each, 1800
            # students is most of a day of failing one at a time — so stop
            # and say why instead of grinding through the whole list. The
            # flags stay raised, so the next run simply picks them all up.
            if i_traek >= _GAAAFSTAND_MAX_I_TRAEK:
                logger.error(
                    "calculate_gaaafstand: %d consecutive failures — giving "
                    "up after %d of %d student(s). Every unmeasured student "
                    "keeps kraever_genberegning = 1, so nothing is lost and "
                    "the next run picks them up. Last reason: %s",
                    i_traek,
                    i,
                    len(candidates),
                    grund,
                )
                break

            continue

        i_traek = 0

        ventende.append((distance_km, row["cpr"]))
        updated += 1

        if config.GAAAFSTAND_VERBOSE:
            logger.info(
                "        skriv: skoleafstand = %.3f, kraever_genberegning "
                "-> 0 %s",
                distance_km,
                "(DRY RUN — skrives ikke)" if config.DRY_RUN else "(i naeste batch)",
            )

        if len(ventende) >= (1 if config.GAAAFSTAND_LIMIT else _GAAAFSTAND_BATCH):
            _skriv(ventende)
            ventende.clear()


    _skriv(ventende)

    # One line per DISTINCT failure, with a few CPRs to chase it with. The
    # whole list is useless at this size and the reason is what matters.
    for grund, cprs in sorted(fejl.items(), key=lambda kv: -len(kv[1])):
        logger.warning(
            "calculate_gaaafstand: %d student(s) skipped — %s. CPR(s): %s%s",
            len(cprs),
            grund,
            ", ".join(cprs[:5]),
            f" (+{len(cprs) - 5} more)" if len(cprs) > 5 else "",
        )

    action = "would write" if config.DRY_RUN else "wrote"
    logger.info(
        "calculate_gaaafstand: done in %s — %s %d distances, %d skipped "
        "across %d distinct reason(s). %d student(s) still flagged for a "
        "later run.",
        _varighed(time.monotonic() - startet),
        action,
        updated,
        skipped,
        len(fejl),
        afventer + skipped,
    )


def _go_credentials() -> tuple[str, str, str]:
    """(endpoint, username, password) for GO, from the RPA credential store.

    The same three values go_journalisering reads. Fetched per run rather than
    held in the environment, so a rotated password takes effect without
    redeploying anything.
    """

    with RPAConnection(db_env="PROD", commit=False) as rpa_conn:
        return (
            rpa_conn.get_constant("go_api_endpoint")["value"].rstrip("/"),
            rpa_conn.get_credential("go_api")["username"],
            rpa_conn.get_credential("go_api")["decrypted_password"],
        )


def _go_case_url(endpoint: str, auth, sag: str) -> str | None:
    """The full GO page URL for one case, or None when it cannot be resolved.

    GET /_goapi/Cases/Metadata/<sag> answers with JSON whose "Metadata" field
    is an XML row. The relative path lives in its ows_CaseUrl attribute:

        ows_CaseUrl="cases/PPR01/PPR-2026-123456"

    PPR01 is a per-case system id, which is why this cannot be composed from
    the case key alone.

    Returns None on anything unexpected — a missing case, a changed response
    shape, a network failure. A missing link is a cosmetic gap that the next
    run retries; a wrong one sends a caseworker into someone else's case.
    """

    try:
        response = requests.get(
            f"{endpoint}/_goapi/Cases/Metadata/{sag}",
            headers={"Content-Type": "application/json"},
            auth=auth,
            timeout=60,
        )
    except requests.RequestException as exc:
        logger.warning("embed_esdh_url: %s — request failed: %s", sag, exc)
        return None

    if not response.ok:
        logger.warning(
            "embed_esdh_url: %s — HTTP %s from GO: %s",
            sag,
            response.status_code,
            response.text[:200].strip(),
        )
        return None

    try:
        metadata = response.json().get("Metadata", "")
        relativ = ET.fromstring(metadata).attrib.get(_GO_CASE_URL_ATTRIB, "")
    except (ValueError, ET.ParseError) as exc:
        logger.warning("embed_esdh_url: %s — could not read metadata: %s", sag, exc)
        return None

    relativ = relativ.strip().strip("/")

    if not relativ:
        logger.warning(
            "embed_esdh_url: %s — GO returned no %s attribute.", sag, _GO_CASE_URL_ATTRIB
        )
        return None

    return f"{endpoint}/{relativ}/{_GO_SIDE}"


def _embed_esdh_url():
    """Fill Bevilling.esdh_url for every bevilling that has a key but no link.

    esdh_noegle is shown in the application as plain text, so a caseworker who
    wants the case in GO has to search for it. The link cannot be built from
    the key, because GO's URL carries a per-case system id — see _go_case_url.

    One lookup per DISTINCT base case, not per bevilling: a student's
    bevillinger share a case, and the sub-case suffix does not change the page
    the link points at.

    Only rows where esdh_url IS NULL are considered, so this is cheap on every
    night after the first. A case whose link cannot be resolved stays NULL and
    is retried tomorrow — the same ratchet the walking distance uses, and for
    the same reason: "not resolved" is true of it, and saying otherwise would
    hide it for good.
    """

    select_sql = """
        SELECT   b.esdh_noegle, COUNT(*) AS antal
        FROM     [befordring].[Bevilling] b
        WHERE    b.aktiv = 1
        AND      b.esdh_url IS NULL
        AND      NULLIF(LTRIM(RTRIM(b.esdh_noegle)), '') IS NOT NULL
        GROUP BY b.esdh_noegle
        ORDER BY b.esdh_noegle
    """

    update_sql = """
        UPDATE [befordring].[Bevilling]
        SET    esdh_url = ?
        WHERE  aktiv = 1
        AND    esdh_url IS NULL
        AND    LTRIM(RTRIM(esdh_noegle)) = ?
    """

    with _connect() as conn:
        cursor = conn.cursor()
        cursor.execute(select_sql)
        rows = _rows_as_dicts(cursor)

    if not rows:
        logger.info("embed_esdh_url: every bevilling with a case key already has a link.")
        return

    # noegle -> base case. A key that is not shaped like a PPR case is left
    # alone rather than guessed at.
    opgaver: dict[str, str] = {}
    ukendt_form = 0

    for row in rows:
        noegle = str(row["esdh_noegle"]).strip()
        traef = _PPR_SAG.match(noegle)

        if traef:
            opgaver[noegle] = traef.group(1)
        else:
            ukendt_form += 1

    logger.info(
        "embed_esdh_url: %d case key(s) without a link, across %d bevilling(er). "
        "%d of them are not shaped like a PPR case and are skipped.",
        len(opgaver),
        sum(r["antal"] for r in rows),
        ukendt_form,
    )

    if not opgaver:
        return

    if config.DRY_RUN:
        for noegle, base in list(opgaver.items())[:10]:
            logger.info("DRY RUN — %s -> would resolve case %s", noegle, base)

        logger.info("DRY RUN — embed_esdh_url: %d key(s), nothing written.", len(opgaver))
        return

    endpoint, brugernavn, kodeord = _go_credentials()
    auth = HttpNtlmAuth(brugernavn, kodeord)

    # No DB connection is held while GO is being called. Same reasoning as
    # _calculate_gaaafstand: an UPDATE takes an exclusive row lock until the
    # commit, and a loop of thousands of external calls under one transaction
    # escalates to a lock on Bevilling — which every page in the application
    # reads. Resolve first, write in short batches.
    url_pr_base: dict[str, str | None] = {}
    ventende: list[tuple[str, str]] = []
    skrevet = 0
    uloeste = 0
    startet = time.monotonic()
    sidste_puls = startet

    def _skriv(batch: list[tuple[str, str]]) -> int:
        """One short transaction per batch — locks held for milliseconds."""

        if not batch:
            return 0

        raekker = 0

        with _connect(autocommit=False) as skrive_conn:
            cursor = skrive_conn.cursor()

            for url, noegle in batch:
                cursor.execute(update_sql, url, noegle)
                raekker += cursor.rowcount

            skrive_conn.commit()

        return raekker

    for i, (noegle, base) in enumerate(opgaver.items(), start=1):
        if base not in url_pr_base:
            url_pr_base[base] = _go_case_url(endpoint, auth, base)

        url = url_pr_base[base]

        if url:
            ventende.append((url, noegle))

            if len(ventende) >= _ESDH_URL_BATCH:
                skrevet += _skriv(ventende)
                ventende.clear()
        else:
            uloeste += 1

        # One GO call per case and a 60-second timeout each: this step runs for
        # minutes on the first night. Silence that long reads as a hang.
        naa = time.monotonic()

        if naa - sidste_puls >= _GAAAFSTAND_HEARTBEAT:
            sidste_puls = naa
            forloebet = naa - startet
            tempo = i / forloebet if forloebet else 0

            logger.info(
                "embed_esdh_url: %d/%d (%.0f%%) — %d linked, %d unresolved, "
                "%.1f/s, %s elapsed, ~%s left",
                i,
                len(opgaver),
                100 * i / len(opgaver),
                skrevet + len(ventende),
                uloeste,
                tempo,
                _varighed(forloebet),
                _varighed((len(opgaver) - i) / tempo if tempo else 0),
            )

    skrevet += _skriv(ventende)

    logger.info(
        "embed_esdh_url: done in %s — resolved %d of %d case(s) in GO and "
        "linked %d bevilling(er). %d key(s) could not be resolved and are "
        "retried on the next run.",
        _varighed(time.monotonic() - startet),
        sum(1 for u in url_pr_base.values() if u),
        len(url_pr_base),
        skrevet,
        uloeste,
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

    "usp_upsert_elev_from_stg": partial(_run_sp, "usp_upsert_elev_from_stg", "upsert_elev"),

    "usp_upsert_foraelder_from_stg": partial(_run_sp, "usp_upsert_foraelder_from_stg", "upsert_foraelder"),

    "_fetch_and_upsert_person_adresser": _fetch_and_upsert_person_adresser,

    "exec_sp": _exec_sp,

    "usp_sync_elev_matrikel_from_bevilling": partial(_run_sp, "usp_sync_elev_matrikel_from_bevilling", "sync_elev_matrikel"),

    "_calculate_gaaafstand": _calculate_gaaafstand,

    "_embed_esdh_url": _embed_esdh_url,
}
