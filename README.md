# ✈️ Flight Booking Analytics — Azure Data Factory ETL Pipeline

An end-to-end, production-style **Azure Data Factory (ADF)** pipeline that ingests multi-format, multi-source flight booking data into a **medallion lakehouse (Bronze → Silver → Gold)** on ADLS Gen2 — featuring **watermark-based incremental loading**, **Mapping Data Flow transformations**, **Delta Lake upserts**, and **automated failure alerting via Logic Apps**.

---

## 📌 Project Overview

This project models an airline booking analytics platform built on a classic **star schema**: four dimension tables (`DimAirline`, `DimFlight`, `DimPassenger`, `DimAirport`) and one fact table (`FactBookings`). The challenge: the data arrives from **heterogeneous sources** — a relational Azure SQL database, flat files versioned in GitHub, and JSON via REST — and downstream analysts need clean, conformed, business-level aggregates, not raw dumps.

A single orchestrating pipeline fans out ingestion, applies incremental loading with watermark tracking, transforms data through Silver into curated Gold business views (top-5 revenue by airline and by flight), and alerts on failure — fully automated on a daily schedule.

**The interview story:**

> Enterprise data rarely lives in one place. This pipeline demonstrates how to build a metadata-driven, fault-tolerant ADF solution that ingests from SQL, REST APIs, and file stores; loads incrementally instead of re-copying everything; cleans and conforms dimensions; and serves analytics-ready Delta tables — with alerting wired in, the way a real production system would have it.

---

## 🏗️ Architecture

```javascript
                    ┌──────────────── Azure Data Factory (adf-projectNew) ────────────────┐
                    │                                                                     │
  ┌──────────┐      │   ┌─────────────────────── ProductionPipeline ───────────────────┐  │
  │ Azure SQL │     │   │  (orchestrator — scheduled daily 02:00 Asia/Dhaka)           │  │
  │ Database  │     │   │                                                              │  │
  │           │     │   │  ┌────────────────┐                                          │  │
  │ FactBook- │     │   │  │ ExecuteADLS    │──► PipelineIngestionFromAdf              │  │
  │ ings      │────►│   │  └──────┬─────────┘      ForEach file_name[]                 │  │
  │ (watermark│     │   │         │                    └─► Copy CSV dims ──► Bronze    │  │
  │  source)  │     │   │         ▼                                                    │  │
  └──────────┘      │   │  ┌────────────────┐                                          │  │
                    │   │  │ ExecuteAPI     │──► APIIngestionFromGit                   │  │
  ┌──────────┐      │   │  └──────┬─────────┘      WebActivity (GET raw GitHub JSON)   │  │
  │ GitHub    │     │   │         │                    └─► Copy (HTTP→ADLS, mapped)    │  │
  │ (CSV dims,│────►│   │         ▼                         ──► Bronze                 │  │
  │  airport  │     │   │  ┌────────────────┐      ┌───────────────────────────┐       │  │
  │  JSON)    │     │   │  │ ExecuteIncr-   │      │  AzureSqlToADLS           │       │  │
  └──────────┘      │   │  │ ementalLoading │─────►│  ┌─ Lookup LastLoadTime    │      │  │
                    │   │  └──────┬─────────┘      │  │   (watermark from ADLS) │      │  │
  ┌──────────┐      │   │         │                │  ├─ Lookup LatestLoadTime  │      │  │
  │ On-prem   │     │   │         │                │  │   (MAX(booking_date))   │      │  │
  │ (via      │     │   │         │                │  ├─ Copy SQL → Parquet     │      │  │
  │ Self-Host │     │   │         │                │  │   WHERE booking_date    │      │  │
  │ ed IR)    │────►│   │         │                │  │   BETWEEN watermarks    │──► Bronze│  │
  └──────────┘      │   │         │                │  └─ Copy: update watermark │      │  │
                    │   │         ▼                └───────────────────────────┘       │  │
                    │   │  ┌────────────────┐                                          │  │
                    │   │  │ ExecuteSilver  │──► silverTransformation                  │  │
                    │   │  └──────┬─────────┘      Mapping Data Flow (8-core):         │  │
                    │   │         │                  normalize text (upper/initCap),   │  │
                    │   │         │                  fix gender codes, cast types,     │  │
                    │   │         │                  alterRow upsert ──► Delta: Silver │  │
                    │   │         ▼                                                    │  │
                    │   │  ┌────────────────┐                                          │  │
                    │   │  │ ExecuteGold    │──► goldDataServing                       │  │
                    │   │  └──────┬─────────┘      Mapping Data Flow (8-core):         │  │
                    │   │         │                  join fact ↔ dims,                 │  │
                    │   │         │                  aggregate revenue,                │  │
                    │   │         │                  denseRank → Top-5,                │  │
                    │   │         │                  handle nulls ──► Delta: Gold      │  │
                    │   │         │                                                    │  │
                    │   │  ┌──────┴─────────┐                                          │  │
                    │   │  │ Alertreqest    │──► WebActivity POST ──► Azure Logic App  │  │
                    │   │  │ (on Failed)    │      (pipeline, run_id, status, error)   │  │
                    │   │  └────────────────┘                                          │  │
                    │   └──────────────────────────────────────────────────────────────┘  │
                    │                                                                     │
                    │   ┌──────────────── ADLS Gen2 (Delta Lake) ───────────────--─┐      │
                    │   │  🥉 bronze  — raw CSV / JSON / Parquet as-ingested       │      │
                    │   │  🥈 silver  — cleaned, conformed, upserted Delta tables │       │
                    │   │  🥇 gold    — BusinessViewAirline, BusinessViewFlight,  │       │
                    │   │               dimAirline, dimFlight                      │       │
                    │   └──────────────────────────────────────────────────────────┘       │
                    └──────────────────────────────────────────────────────────────────────┘
```

---

## ⭐ Data Model — Star Schema

| Table | Type | Grain | Key Columns |
| --- | --- | --- | --- |
| `DimAirline` | Dimension | One row per airline | `airline_id`, `airline_name`, `country` |
| `DimFlight` | Dimension | One row per flight | `flight_id`, `flight_number`, `departure_time`, `arrival_time` |
| `DimPassenger` | Dimension | One row per passenger | `passenger_id`, `full_name`, `gender`, `age`, `country` |
| `DimAirport` | Dimension | One row per airport | `airport_id`, `airport_name`, `city`, `country` |
| `FactBookings` | Fact | One row per booking | `booking_id`, passenger/flight/airline/airport FKs, `booking_date`, `ticket_cost`, `flight_duration_mins`, `checkin_status` |

---

## 🧰 Tech Stack

| Layer | Technology | Purpose |
| --- | --- | --- |
| **Orchestration / ETL** | Azure Data Factory | Pipelines, triggers, control flow, Copy activities |
| **Transformations** | ADF Mapping Data Flows | Code-free Spark transformations (derive, cast, aggregate, window, alterRow) |
| **Storage** | ADLS Gen2 + Delta Lake | Bronze/Silver/Gold zones; ACID upserts via Delta |
| **Relational source** | Azure SQL Database | `FactBookings` transactional fact table |
| **REST / file sources** | GitHub raw content (HTTP) | Dimension CSVs and airport JSON |
| **Connectivity** | Self-Hosted Integration Runtime (`rafiu`) | On-premises data movement capability |
| **Alerting** | Azure Logic Apps + ADF WebActivity | Failure notifications with run context |
| **CI-friendly** | ADF Git integration | All pipelines/datasets/dataflows version-controlled in this repo |

---

## 📁 Repository Structure

```javascript
ADF_Project/
├── pipeline/                          # ADF pipelines (orchestration layer)
│   ├── ProductionPipeline.json        #   ⭐ Master orchestrator — fans out 4 child
│   │                                  #      pipelines + failure alerting
│   ├── PipelineIngestionFromAdf.json  #   Parameterized ForEach CSV ingestion → Bronze
│   ├── APIIngestionFromGit.json       #   REST ingestion of airport JSON → Bronze
│   ├── AzureSqlToADLS.json            #   ⭐ Watermark-based incremental SQL → Parquet
│   ├── silverTransformation.json      #   Wrapper: runs Silver Data Flow
│   └── goldDataServing.json           #   Wrapper: runs Gold Data Flow
├── dataflow/                          # Mapping Data Flows (transformation layer)
│   ├── SilverTransformation.json      #   Cleaning + conforming 4 dims + fact → Delta upsert
│   └── goldDataServing.json           #   Joins, revenue aggregation, Top-5 ranking → Gold
├── dataset/                           # 15+ datasets (sources, sinks, watermark, params)
│   ├── waterMark.json / MonitorOldData.json   # Watermark storage (ADLS JSON)
│   ├── SqlToParquet.json                      # SQL → Bronze Parquet sink
│   ├── silver_*.json                          # Silver Delta sources
│   └── ...
├── linkedService/
│   ├── AzureSqlDatabase1.json         # Azure SQL source
│   ├── AzureDataLakeStorage1.json     # ADLS Gen2 lakehouse
│   └── GitToAzure.json                # GitHub HTTP source
├── integrationRuntime/
│   └── rafiu.json                     # Self-hosted IR (on-prem migration)
├── trigger/
│   └── triggerTriggerProdPipeline.json# Daily 02:00 schedule (Asia/Dhaka)
├── SourceFiles/                       # Sample / seed data committed to the repo
│   ├── CSV/      (DimAirline, DimFlight, DimPassenger)
│   ├── JSON/     (DimAirport)
│   └── SQL/      (fact_bookings_full.sql — booking fact seed data)
├── factory/adf-projectNew.json        # Factory config (SystemAssigned managed identity)
└── publish_config.json
```

---

## ⚙️ How the Pipeline Works

### 1️⃣ Ingestion — Three Patterns, One Orchestrator

The master **`ProductionPipeline`** executes child pipelines sequentially (`waitOnCompletion: true`), each solving a different ingestion problem:

| Child Pipeline | Pattern | Source → Sink |
| --- | --- | --- |
| `PipelineIngestionFromAdf` | **Parameterized batch** — `ForEach` loops over a `file_name[]` pipeline parameter, so the file list is data, not code | GitHub CSV dims → Bronze |
| `APIIngestionFromGit` | **REST ingestion** — `WebActivity` validates the endpoint, then `Copy` pulls JSON over HTTP with an explicit field mapping (`airport_id`, `airport_name`, `city`, `country`) | GitHub raw JSON → Bronze |
| `AzureSqlToADLS` | **Incremental (watermark)** — see below | Azure SQL `FactBookings` → Bronze Parquet |

### 2️⃣ Incremental Loading — The Watermark Pattern

Full re-loads don't scale. This pipeline tracks state in a small JSON watermark file (`MonitorOldData`) in ADLS:

1. **Lookup `LastLoadTime`** — reads the previous high-water mark from ADLS.
2. **Lookup `LatestLoadTime`** — queries `SELECT MAX(booking_date) FROM dbo.FactBookings` for the current ceiling.
3. **Copy** — pulls only the delta: `WHERE booking_date > @lastload AND booking_date <= @latestload`, written as columnar **Parquet**.
4. **Update watermark** — a second Copy writes the new `lastload` value back to ADLS, ready for the next run.

Result: each scheduled run moves only new/changed rows — O(delta) instead of O(table).

### 3️⃣ Silver — Clean & Conform (Mapping Data Flow)

`SilverTransformation` reads all five Bronze entities and standardizes them:

- **Text normalization** — `upper(country)`, `initCap(airline_name / airport_name / city / full_name)`
- **Domain fixes** — gender codes normalized: `m → Male`, `f → Female`
- **Type safety** — `ticket_cost` cast to integer with error routing
- **Idempotent writes** — `alterRow(upsertIf(1==1))` into **Delta Lake** keyed on business keys (`airline_id`, `flight_id`, `passenger_id`, `airport_id`, `booking_id`) — reruns are safe, duplicates are merged

### 4️⃣ Gold — Business Views (Mapping Data Flow)

`goldDataServing` turns conformed data into decisions:

- **Top-5 Airlines by Revenue** — join `factBookings ⨝ dimAirline` → sum `ticket_cost` per airline → `denseRank()` window → filter rank ≤ 5 → `gold/BusinessViewAirline`
- **Top-5 Flights by Revenue** — join `factBookings ⨝ dimFlight` → **null handling** (`iifNull(flight_number, 'Unknown')`) → revenue aggregation → ranking → `gold/BusinessViewFlight`
- Dimensions also promoted to Gold for serving

### 5️⃣ Failure Alerting

A **`WebActivity`** fires on `Failed` dependency conditions from the incremental, Silver, or Gold stages and POSTs the pipeline name, run ID, status, and error message to an **Azure Logic App** — so breakage is pushed to the team instead of discovered in a dashboard.

### 6️⃣ Scheduling

A daily **Schedule Trigger** (02:00 Asia/Dhaka) passes the dimension file list into the master pipeline. *(Currently stopped in the repo — start it to activate the schedule.)*

---

## 🎯 Key Design Decisions (Interview Talking Points)

1. **Watermark incremental loading** — tracking `MAX(booking_date)` state in ADLS avoids full-table scans and shrinks each run to true deltas. Be ready to discuss edge cases: late-arriving rows, deletes, and multiple watermark columns.
2. **Parameter-driven ingestion** — the file list is a pipeline *parameter* (an array of objects), so adding a dimension file requires zero pipeline edits — the trigger just passes a longer list.
3. **Delta Lake as the Silver/Gold store** — `alterRow` upserts keyed on business keys make transformations idempotent; a failed rerun can't create duplicates.
4. **Separation of orchestration and transformation** — thin wrapper pipelines (`ExecuteDataFlow`) keep control flow and data flow independently testable and reusable.
5. **Fault-aware orchestration** — the alerting activity depends on `Failed` conditions (not success), demonstrating conditional branching and production-grade operability.
6. **Schema flexibility** — sources declare `allowSchemaDrift: true`, so evolving upstream files don't break the flow; Silver enforces the contract.
7. **Managed identity security** — the factory uses a `SystemAssigned` identity rather than stored credentials in linked services.

---

## 🚀 Deployment

### Prerequisites

- Azure subscription with: **Data Factory**, **ADLS Gen2** (with `bronze`, `silver`, `gold` containers), **Azure SQL Database**, **Logic App** (for alerts)
- The ADF factory Git-connected to this repository (publish branch)

### Steps

1. **Recreate linked services** in your factory (`AzureSqlDatabase1`, `AzureDataLakeStorage1`, `GitToAzure`) — connection strings are environment-specific and intentionally not committed.
2. **Deploy the SQL seed data**: run `SourceFiles/SQL/fact_bookings_full.sql` against your Azure SQL DB.
3. **Seed the watermark**: place an initial `MonitorOldData` JSON in the ADLS `bronze` area (e.g. `{"lastload": "2020-01-01"}`).
4. **Publish** the factory — pipelines, datasets, and dataflows deploy from the `adf-projectNew` publish branch.
5. **Configure the alert endpoint**: point the `Alertreqest` WebActivity at your Logic App HTTP trigger (store the URL in **Azure Key Vault** — see note below).
6. **Start the trigger** `triggerTriggerProdPipeline`.

---

## 🧪 Sample Data

**FactBookings (Azure SQL):**

```sql
booking_id | passenger_id | flight_id | airline_id | origin_airport_id | destination_airport_id | booking_date | ticket_cost | flight_duration_mins | checkin_status
```

**DimAirport (GitHub JSON):**

```json
{ "airport_id": 1, "airport_name": "Hazrat Shahjalal Intl", "city": "Dhaka", "country": "Bangladesh" }
```

---

## 🚧 Challenges & Learnings

- **Watermark bookkeeping** — the hardest part of incremental loads isn't the query, it's *state*: the pipeline reads the old mark, computes the new ceiling, loads the window, and only then persists the new mark — ordering that guarantees no row is ever skipped or loaded twice.
- **Idempotent Silver writes** — switching sinks to Delta `upsert` keyed on business keys turned reruns from "dangerous" into "free retries."
- **Nulls in business logic** — a missing `flight_number` would silently drop revenue rows in an inner flow; an explicit `iifNull(..., 'Unknown')` derive keeps them visible in the Top-5 ranking.
- **Heterogeneous sources, one contract** — CSV, JSON-over-REST, and SQL all land in Bronze as files; the Silver flow doesn't care where a row came from — that's the point of the medallion layer.

---

## 🔮 Future Improvements

- [ ] Move the Logic App alert URL into **Azure Key Vault** (the SAS-signed URL is currently visible in pipeline JSON — a security gap to close)
- [ ] **Tumbling window trigger** with dependency chains for backfill-safe reruns
- [ ] Data quality gates (e.g., assert non-null keys / positive fares in Silver) before Gold
- [ ] Parameterize the watermark column to support multiple incremental tables generically
- [ ] **CI/CD with Azure DevOps / GitHub Actions** — ARM template export + automated publish
- [ ] Serve Gold Delta tables to **Power BI** semantic models

---

## 📜 License

Released under the MIT License.

---

> Built as a portfolio project demonstrating production ADF patterns: parameterized ingestion, REST-based loads, watermark incremental ETL, Mapping Data Flow transformations, Delta Lake upserts, and failure alerting.
