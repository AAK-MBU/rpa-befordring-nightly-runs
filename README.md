# rpa-befordring-nightly-runs

Robot der kører natligt og fanger datobaserede ændringer, der påvirker befordring (Aarhus Kommune, MBU).

Bygget på [odense-rpa/process-template](https://github.com/odense-rpa/process-template) og drevet af Automation Server (ATS): robotten lægger nattens opgaver i en workqueue og afvikler dem derefter mod `befordring`-skemaet i *Befordringssystemet*.

## Krav

- Python 3.13
- [uv](https://docs.astral.sh/uv/)
- ODBC-driver til SQL Server (`pyodbc`)
- En `.env` med ATS- og databaseoplysninger (se nedenfor)

## Kom i gang

```sh
uv sync
```

## Kørsel

Processen består af tre faser, som vælges med flag. Flagene kan kombineres og afvikles altid i rækkefølgen queue → process → finalize:

```sh
uv run python main.py --queue      # fylder workqueuen
uv run python main.py --process    # afvikler items i workqueuen
uv run python main.py --finalize   # efterbehandling
```

> **Bemærk:** `main.py` rydder workqueuen ved opstart, uanset hvilket flag der bruges. Kør derfor faserne i samme kald (`--queue --process --finalize`) — et separat `--process`-kald tømmer den kø, det skulle afvikle.

### Hvad laver nattens opgaver?

`retrieve_items_for_queue()` danner ét enkelt item, `{dato}_nightly_run`. Trinnene nedenfor afhænger af hinanden i rækkefølge, og separate workqueue-items giver ingen garanti for rækkefølgen. `process_item()` sender videre ud fra `action`, og hvert trin kan stadig køres enkeltvis ved at lægge et item i køen med det pågældende trins action-navn:

| Trin | Hvad | Hvor |
|---|---|---|
| 1 | Adresser: `LOIS.DAR.AdresseDkGeoView` → `Adresse_STG` → `Adresse` | `_fetch_and_upsert_addresses` + `usp_upsert_adresser_from_stg` |
| 2 | `Elev_STG` → `Elev` (ikke matrikel_id / ungdomsuddannelse_id / adresse_id / skoleafstand / kraever_genberegning) | `usp_upsert_elev_from_stg` |
| 3 | `Foraelder_STG` → `Foraelder` (ikke adresse_id / maa_vide_barns_adresse) | `usp_upsert_foraelder_from_stg` |
| 4 | `adresse_id` på elever og forældre: `LOIS.CPR.PersonGeoView` → `Elev_Adresse_STG` → begge tabeller | `_fetch_and_upsert_person_adresser` + `usp_upsert_adresse_ids_from_stg` |
| 5 | Genberegn status på alle bevillinger; markér revurdering / genbehandling | `usp_recalculate_bevilling_status` |
| 6 | Udled elevens skole fra bevillingen | `usp_sync_elev_matrikel_from_bevilling` |
| 7 | Gåafstand for alle markerede; rydder markeringen | `_calculate_gaaafstand` |

Trin 5 kører **før** trin 6 med vilje: det er trin 5, der gør Kommende til Aktiv, og trin 6 spørger først og fremmest, hvilken bevilling der er aktiv. Bytter man om, når den nyaktiverede bevillings skole først eleven næste nat.

`kraever_genberegning` sættes af det trin, der ejer den ændrede kolonne — skolekode i trin 2, adresse_id i trin 4, skole i trin 6 — og ryddes kun af trin 7.

Fejl håndteres pr. item: en `BusinessError` sætter item'et til `pending_user`, mens en `ProcessError` fejler item'et, sender en fejlmail med skærmbillede og tæller mod `MAX_RETRY` (10).

### `Elev` skrives ikke herfra

`_exec_sp` synkroniserede tidligere Kommende→Aktiv-overgange ned på `Elev` og kørte SP'en igen, så overgangen ikke udløste et flag. Det er fjernet. Det virkede aldrig (den skrev `Elev.matrikel_id`, men SP'en sammenligner `Elev.skolekode`), og den undertrykte noget, der nu er meningen at man skal se: efter opdelingen af revurdering og genbehandling udløser en skolekode-uoverensstemmelse **genbehandling**, som står, indtil en sagsbehandler markerer den som håndteret.

En kommende bevilling, der aktiveres på en anden skole end elevens registrerede, havner derfor på genbehandlingssiden. Det er forventeligt, når en elev skifter skole, og det koster ét klik at rydde.

Reglen bag: data-workeren ejer `Elev` og skriver de gældende værdier dertil. Intet i dette repo skriver `Elev` ud fra en bevilling.

## Konfiguration

Tunables ligger i `helpers/config.py` — bl.a. `MAX_RETRY`, `MAX_CONCURRENCY`, `RETRY_BASE_DELAY` og `DRY_RUN`. Med `DRY_RUN = True` kalder `_exec_sp` sin stored procedure med `@dry_run = 1` og committer ikke; adresseimporten skriver dog stadig.

### Miljøvariabler (`.env`)

| Variabel | Formål |
|---|---|
| `ATS_URL` | Base-URL til Automation Server API |
| `ATS_TOKEN` | Bearer-token til ATS |
| `ATS_WORKQUEUE_OVERRIDE` | Overskriver workqueue-id (dev/test) |
| `DBCONNECTIONSTRINGBEFORDRING` | Forbindelse brugt af SQLAlchemy-motoren i `helpers/db.py` |
| `DBCONNECTIONSTRINGSERVER29` | LOIS-kildeserver til adresseimporten |
| `DBCONNECTIONSTRINGDEV` | Målserver (bruges i dag til både kilde og mål) |
| `DBCONNECTIONSTRINGPROD` | Produktionsforbindelse |
| `API_ENDPOINT` / `API_KEY` | Backend til gåafstand (kun brugt af den udkommenterede kode) |

SMTP-konstanter (`Error Email`, `Email Friend`, `smtp_server`, `smtp_port`) hentes af `mbu-dev-shared-components` fra `RPAConnection` mod `PROD`.

## Udvikling

```sh
uv run ruff check .
uv run ruff format .
```

GitHub Actions kører `ruff check` ved hvert push og pull request, og en PR mod `main` afvises, hvis `version` i `pyproject.toml` ikke er hævet.

## Før deployment

- Fjern SSL-bypass-blokken øverst i `main.py` (markeret **REMOVE BEFORE DEPLOYMENT**) — den slår certifikatvalidering fra for alle `requests`-kald.
- Ret adresseimporten, så kilden er Server 29 og ikke DEV, og fjern `SELECT top (10)` i `fetch_sql`.
- Fjern `SELECT top (10)` og tilsvarende begrænsninger i LOIS-forespørgslerne.
