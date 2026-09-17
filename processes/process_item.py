"""Module to handle item processing"""

import logging
import os
import uuid

import pyodbc
import requests

from mbu_rpa_core.exceptions import BusinessError
from sqlalchemy import text

from helpers import config
from helpers.db import get_db

logger = logging.getLogger(__name__)


def process_item(item_data: dict, item_reference: str):
    """Dispatch processing based on the action in item_data."""

    assert item_data, "Item data is required"
    assert item_reference, "Item reference is required"

    action = item_data.get("action")

    if action == "exec_sp":
        _exec_sp()
    elif action == "_fetch_and_upsert_addresses":
        _fetch_and_upsert_addresses()
    else:
        raise BusinessError(f"Unknown action: {action}")


def _exec_sp():
    """Run usp_recalculate_bevilling_status for all bevillinger (no bevilling_id).

    Skolekode-mismatch detection is built into the SP, so a single nightly
    call covers both status recalculation and data-difference checks.

    In dry-run mode the SP is called with @dry_run = 1, which returns the
    same result set but does not commit any changes to the database.

    Post-processing (Kommende→Aktiv sync):
    When the SP activates a Kommende bevilling, the data worker (which runs
    before the RPA) cannot see the upcoming transition and therefore never
    sets kraever_genberegning for the matrikel_id change. After the main SP
    run we detect these transitions and:
      1. Update Elev.matrikel_id / ungdomsuddannelse_id to the newly-active
         bevilling's school so calculate_gaaafstand has the correct coordinates.
      2. Set kraever_genberegning = 1 so the walking distance is recalculated
         tonight rather than a full day later.
      3. Re-run the SP for each affected bevilling as a safety net — with Elev
         now in sync, any transient Revurdering from a temporary skolekode
         mismatch is corrected to Aktiv before the night ends.
    """

    dry_run_flag = 1 if config.DRY_RUN else 0

    if config.DRY_RUN:
        logger.info("DRY RUN — exec_sp: calling SP with @dry_run = 1 (no writes)")

    sql = text("""
        EXEC [befordring].[usp_recalculate_bevilling_status]
            @bevilling_id = :bevilling_id,
            @today        = :today,
            @dry_run      = :dry_run
    """)

    with get_db() as db:
        result = db.execute(sql, {"bevilling_id": None, "today": None, "dry_run": dry_run_flag})
        rows = [dict(row) for row in result.mappings().all()]
        if not config.DRY_RUN:
            db.commit()

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

    # -----------------------------------------------------------------------
    # Kommende→Aktiv post-processing
    # -----------------------------------------------------------------------
    kommende_activated = [
        r for r in rows
        if r.get("current_status_text") == "Kommende" and r.get("status_will_change")
    ]

    if not kommende_activated:
        return

    logger.info(
        "exec_sp: %d bevilling(er) transitioned from Kommende — syncing Elev.matrikel_id",
        len(kommende_activated),
    )

    bevilling_lookup_sql = text("""
        SELECT
            b.cpr_elev,
            b.matrikel_id,
            b.ungdomsuddannelse_id
        FROM [befordring].[Bevilling] b
        WHERE b.bevilling_id = :bevilling_id
    """)

    elev_update_sql = text("""
        UPDATE [befordring].[Elev]
        SET    matrikel_id          = :matrikel_id,
               ungdomsuddannelse_id = :ungdomsuddannelse_id,
               kraever_genberegning = 1
        WHERE  cpr = :cpr
    """)

    # Safety-net re-run: with Elev now in sync, the SP re-evaluates the
    # bevilling without a skolekode mismatch and confirms Aktiv.
    rerun_sql = text("""
        EXEC [befordring].[usp_recalculate_bevilling_status]
            @bevilling_id = :bevilling_id,
            @today        = NULL,
            @dry_run      = :dry_run
    """)

    synced = 0

    with get_db() as db:
        for r in kommende_activated:
            bevilling_id = r.get("bevilling_id")

            bev_row = db.execute(
                bevilling_lookup_sql, {"bevilling_id": bevilling_id}
            ).mappings().first()

            if bev_row is None:
                logger.warning(
                    "exec_sp: bevilling_id=%s not found — skipping Elev sync",
                    bevilling_id,
                )
                continue

            cpr                  = bev_row["cpr_elev"]
            matrikel_id          = bev_row["matrikel_id"]
            ungdomsuddannelse_id = bev_row["ungdomsuddannelse_id"]

            if config.DRY_RUN:
                logger.info(
                    "DRY RUN — exec_sp: would update Elev cpr=%s → "
                    "matrikel_id=%s, ungdomsuddannelse_id=%s, kraever_genberegning=1",
                    cpr, matrikel_id, ungdomsuddannelse_id,
                )
            else:
                rows_affected = db.execute(
                    elev_update_sql,
                    {
                        "cpr": cpr,
                        "matrikel_id": matrikel_id,
                        "ungdomsuddannelse_id": ungdomsuddannelse_id,
                    },
                ).rowcount

                if rows_affected == 0:
                    logger.warning(
                        "exec_sp: UPDATE Elev affected 0 rows for cpr=%s "
                        "(no Elev record?) — skipping SP re-run for bevilling_id=%s",
                        cpr, bevilling_id,
                    )
                    continue

            # Re-run the SP for this bevilling.  Since the Elev UPDATE is in
            # the same open transaction, the SP can see the corrected
            # matrikel_id and will not raise a false Revurdering.
            db.execute(
                rerun_sql,
                {"bevilling_id": bevilling_id, "dry_run": dry_run_flag},
            ).fetchall()

            synced += 1
            logger.info(
                "exec_sp: synced bevilling_id=%s (cpr=%s) → "
                "matrikel_id=%s, kraever_genberegning=1, SP re-run complete",
                bevilling_id, cpr, matrikel_id,
            )

        if not config.DRY_RUN:
            db.commit()

    action = "would sync" if config.DRY_RUN else "synced"
    logger.info(
        "exec_sp: Kommende→Aktiv post-processing complete: %s %d bevilling(er)",
        action, synced,
    )


def _fetch_and_upsert_addresses():
    """
    Fetches addresses from LOIS on Server 29,
    inserts them into Adresse_STG on DEV,
    and then calls the stored procedure that upserts into Adresse.
    """

    conn_string_29 = os.getenv("DBCONNECTIONSTRINGSERVER29")
    conn_string_prod = os.getenv("DBCONNECTIONSTRINGPROD")
    conn_string_dev = os.getenv("DBCONNECTIONSTRINGDEV")

    # if not conn_string_29:
    #     raise ValueError("Missing environment variable: DBCONNECTIONSTRINGSERVER29")

    # if not conn_string_prod:
    #     raise ValueError("Missing environment variable: DBCONNECTIONSTRINGSERVER29")

    # if not conn_string_dev:
    #     raise ValueError("Missing environment variable: DBCONNECTIONSTRINGPROD")

    load_id = str(uuid.uuid4())

    # fetch_sql = """
    #     SELECT top (10)
    #         CONVERT(NVARCHAR(36), [AdresseId]) AS adresse_id,
    #         [AdresseBetegnelse] AS adresse_tekst,
    #         CAST([Lat] AS FLOAT) AS latitude,
    #         CAST([Long] AS FLOAT) AS longitude
    #     FROM [LOIS].[DAR].[AdresseDkGeoView];
    # """

    fetch_sql = """
        SELECT *
        FROM [befordring_app].[befordring].[Adresse];
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

    with pyodbc.connect(conn_string_dev) as source_conn, pyodbc.connect(conn_string_dev) as target_conn:
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


# def _calculate_gaaafstand():
#     """Recalculate walking distance (skoleafstand) for students flagged by the
#     data worker's nightly job.

#     The data worker sets kraever_genberegning = 1 on Elev whenever it detects
#     a change in adresse_id, skolekode, or matrikel_id.  It also writes the
#     authoritative current values for those fields directly onto Elev, so we
#     do NOT sync anything from Bevilling — Elev's own data is the truth here.

#     Steps:
#       1. SELECT students with kraever_genberegning = 1, pulling home address
#          coordinates from Adresse and school coordinates from Skolematrikel or
#          Ungdomsuddannelse (whichever is set on Elev).
#       2. For each, call the backend walking-distance endpoint.
#       3. Write the returned distance to Elev.skoleafstand and reset
#          kraever_genberegning = 0 in a single UPDATE per student.
#     """

#     api_base = os.getenv("API_ENDPOINT", "").rstrip("/")
#     api_key  = os.getenv("API_KEY", "")
#     headers  = {"X-API-Key": api_key}

#     # -----------------------------------------------------------------------
#     # 1. Find all students the data worker has flagged for recalculation.
#     #    School coordinates come from Elev's own matrikel_id /
#     #    ungdomsuddannelse_id — NOT from the bevilling.
#     # -----------------------------------------------------------------------
#     select_sql = text("""
#         SELECT
#             e.cpr,
#             ad.latitude                              AS addr_lat,
#             ad.longitude                             AS addr_lon,
#             COALESCE(sm.latitude,  uu.latitude)      AS school_lat,
#             COALESCE(sm.longitude, uu.longitude)     AS school_lon
#         FROM      [befordring].[Elev]              e
#         JOIN      [befordring].[Adresse]            ad
#                   ON  ad.adresse_id = e.adresse_id
#         LEFT JOIN [befordring].[Skolematrikel]      sm
#                   ON  sm.matrikel_id = e.matrikel_id
#         LEFT JOIN [befordring].[Ungdomsuddannelse]  uu
#                   ON  uu.ungdomsuddannelse_id = e.ungdomsuddannelse_id
#         WHERE e.kraever_genberegning = 1
#     """)

#     # -----------------------------------------------------------------------
#     # 3. Write calculated distance and clear the flag in one statement.
#     # -----------------------------------------------------------------------
#     update_sql = text("""
#         UPDATE [befordring].[Elev]
#         SET    skoleafstand         = :distance,
#                kraever_genberegning = 0
#         WHERE  cpr = :cpr
#     """)

#     if config.DRY_RUN:
#         logger.info("DRY RUN — calculate_gaaafstand: will log candidates and distances but write nothing")

#     with get_db() as db:
#         candidates = [dict(r) for r in db.execute(select_sql).mappings().all()]
#         logger.info("calculate_gaaafstand: %d students flagged for distance recalculation", len(candidates))

#         if not candidates:
#             return

#         # -------------------------------------------------------------------
#         # 2. Call the walking-distance API for each candidate
#         # -------------------------------------------------------------------
#         updated = 0
#         skipped = 0

#         for row in candidates:
#             school_lat = row["school_lat"]
#             school_lon = row["school_lon"]

#             if school_lat is None or school_lon is None:
#                 logger.warning(
#                     "calculate_gaaafstand: no school coordinates for CPR %s "
#                     "(matrikel_id / ungdomsuddannelse_id may be NULL on Elev) — skipping",
#                     row["cpr"],
#                 )
#                 skipped += 1
#                 continue

#             try:
#                 resp = requests.get(
#                     f"{api_base}/bevilling/calculate_walking_distance",
#                     params={
#                         "lat1": row["addr_lat"],
#                         "lon1": row["addr_lon"],
#                         "lat2": school_lat,
#                         "lon2": school_lon,
#                     },
#                     headers=headers,
#                     timeout=15,
#                 )
#                 resp.raise_for_status()
#                 distance_km = resp.json()["distance_km"]
#             except Exception as exc:
#                 logger.warning(
#                     "calculate_gaaafstand: distance API failed for CPR %s — %s",
#                     row["cpr"],
#                     exc,
#                 )
#                 skipped += 1
#                 continue

#             if config.DRY_RUN:
#                 logger.info(
#                     "DRY RUN — CPR %s: would set skoleafstand = %.3f km, kraever_genberegning → 0",
#                     row["cpr"],
#                     distance_km,
#                 )
#             else:
#                 db.execute(update_sql, {"cpr": row["cpr"], "distance": distance_km})
#             updated += 1

#         if not config.DRY_RUN:
#             db.commit()

#     action = "would write" if config.DRY_RUN else "wrote"
#     logger.info(
#         "calculate_gaaafstand: %s %d distances, %d skipped (no school coords or API error)",
#         action,
#         updated,
#         skipped,
#     )
