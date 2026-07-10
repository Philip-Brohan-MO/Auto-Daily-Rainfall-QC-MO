# Auto-Daily-Rainfall-QC

Follow-on to Auto-Daily-Rainfall focused on data identification and quality
control.

## Rainfall Rescue SQLite Ingestion

This repository now includes code to ingest combined station CSV files from a
local clone of Rainfall-Rescue into SQLite for fast search and retrieval.

The ingestion code is in:

- `src/rainfall_rescue_sqlite/parser.py`
- `src/rainfall_rescue_sqlite/schema.py`
- `src/rainfall_rescue_sqlite/ingest.py`
- `scripts/build_rainfall_rescue_sqlite.py`

### Scope

- Ingests combined station files under `Rainfall-Rescue/DATA`
- Excludes source transcription sheets (`TYRain_*.csv`, `ERROR_*.csv`,
	`MISFILED_*.csv`)
- Rebuilds the database from scratch each run

### Environment

Use the ADRQ conda environment defined in `environments/ADRQ.yml`.

If using `conda activate`:

```bash
conda activate ADRQ
```

### Build The SQLite Database

Default behavior uses:

- Rainfall-Rescue root: `${PDIR}/Rainfall-Rescue`
- Output DB path: `${PDIR}/Rainfall-Rescue/rainfall_rescue.sqlite`

Run full rebuild:

```bash
python scripts/build_rainfall_rescue_sqlite.py
```

Optional overrides:

```bash
python scripts/build_rainfall_rescue_sqlite.py \
	--rainfall-rescue-root /data/scratch/philip.brohan/ADRQ/Rainfall-Rescue \
	--db-path /data/scratch/philip.brohan/ADRQ/Rainfall-Rescue/rainfall_rescue.sqlite
```

Smoke test on a subset of files:

```bash
python scripts/build_rainfall_rescue_sqlite.py --max-files 25 --db-path /var/tmp/rainfall_rescue_smoke.sqlite
```

### Tables

- `stations`: one row per ingested combined CSV
- `monthly_rainfall`: one row per station/year/month non-blank value
- `annual_totals`: one row per station/year non-blank total value
- `ingestion_runs`: run-level audit metadata
- `ingestion_file_errors`: per-file parse/insert failures

### Example Queries

```sql
SELECT COUNT(*) FROM stations;
SELECT COUNT(*) FROM monthly_rainfall;
SELECT COUNT(*) FROM annual_totals;
```

```sql
SELECT s.location_name, m.year, m.month, m.rainfall_in
FROM monthly_rainfall m
JOIN stations s ON s.station_file_id = m.station_file_id
WHERE s.location_name LIKE 'ABERPORTH%'
ORDER BY m.year, m.month
LIMIT 24;
```

### Notebook Integration

`notebooks/get_rainfall_rescue_data.ipynb` includes cells to:

- run the full SQLite rebuild
- print row counts
- run a sample station query

## Ensemble Transcription SQLite Ingestion

The daily rainfall ensemble transcriptions are a separate data source, stored in
their own SQLite database. Each JSON file has keys `Day 1` .. `Day 31` plus a
`Totals` block; every key maps to 12 month slots (January-December) and each
month slot holds 5 ensemble member values (rainfall in mm, or `null`).

The ingestion code is in:

- `src/rainfall_rescue_sqlite/ensemble_parser.py`
- `src/rainfall_rescue_sqlite/ensemble_schema.py`
- `src/rainfall_rescue_sqlite/ensemble_ingest.py`
- `scripts/build_ensemble_transcriptions_sqlite.py`

### Scope

- Ingests all `*.json` ensemble transcription files under the source directory
- Stores per-day ensemble values and per-month ensemble totals
- Rebuilds the database from scratch each run
- Kept in a separate database from the Rainfall Rescue monthly data

### Build The SQLite Database

Default behavior uses:

- Ensemble root:
	`/data/scratch/philip.brohan/documents/Daily_Rainfall_UK/operational_sample/ensemble_transcriptions`
	(override with `--ensemble-root` or the `ENSEMBLE_TRANSCRIPTIONS_ROOT`
	environment variable)
- Output DB path: `${PDIR}/ensemble_transcriptions.sqlite`

Run full rebuild:

```bash
python scripts/build_ensemble_transcriptions_sqlite.py
```

Smoke test on a subset of files:

```bash
python scripts/build_ensemble_transcriptions_sqlite.py \
	--max-files 50 --db-path /var/tmp/ensemble_smoke.sqlite
```

### Tables

- `ensemble_files`: one row per ingested JSON file, with filename-derived
	metadata (`year_start`, `year_end`, `descriptor`, `section_id`, `num_days`)
- `ensemble_daily_values`: one row per file/day/month/ensemble member
- `ensemble_monthly_totals`: one row per file/month/ensemble member (from the
	`Totals` block)
- `ensemble_ingestion_runs`: run-level audit metadata
- `ensemble_ingestion_file_errors`: per-file parse/insert failures

### Example Queries

```sql
SELECT COUNT(*) FROM ensemble_files;
SELECT COUNT(*) FROM ensemble_daily_values;
SELECT COUNT(*) FROM ensemble_monthly_totals;
```

```sql
-- Mean ensemble total per month for one file
SELECT f.file_name, t.month, AVG(t.total) AS mean_total
FROM ensemble_monthly_totals t
JOIN ensemble_files f ON f.file_id = t.file_id
WHERE f.file_name = 'DRain_1921-1930_RainNos_Gloucestershire_C-S-435.json'
GROUP BY f.file_name, t.month
ORDER BY t.month;
```

```sql
-- Daily ensemble spread for a specific day and month
SELECT day_of_month, month, ensemble_member, rainfall
FROM ensemble_daily_values
WHERE file_id = 1 AND day_of_month = 1
ORDER BY month, ensemble_member;
```

### Notebook Integration

`notebooks/get_rainfall_rescue_data.ipynb` includes a dedicated section to:

- run the full ensemble SQLite rebuild
- print file / daily / total row counts
- show sample daily ensemble values for one file
- show mean monthly totals for that file

## Cross-Source Monthly Similarity Baseline

This baseline compares monthly Rainfall Rescue station-year profiles against
monthly ensemble transcriptions, ranking candidates by exact month agreement.

### Comparison Design

- RR vectors: 12-month station-year values from `monthly_rainfall`
- Ensemble member values: all 5 monthly member totals per file from
	`ensemble_monthly_totals`
- Primary rank score: count of months where RR equals any ensemble member,
	after rounding both values to 2 decimal places
- Tie-break: larger overlap month count
- Compatibility metrics: masked cosine and adjusted score are still stored for
	diagnostics and historical comparison

### Build And Match

Run full vector build + matching:

```bash
python scripts/run_monthly_similarity_baseline.py
```

Smoke test with small subsets:

```bash
python scripts/run_monthly_similarity_baseline.py \
	--comparison-db-path /var/tmp/monthly_similarity_smoke.sqlite \
	--max-ensemble-queries 100 \
	--max-rr-candidates 5000 \
	--batch-size 8192 \
	--top-k 10 \
	--min-overlap 10
```

Reuse an existing comparison DB and only rerun matching:

```bash
python scripts/run_monthly_similarity_baseline.py \
	--skip-build \
	--comparison-db-path /data/scratch/philip.brohan/ADRQ/monthly_similarity.sqlite
```

### Comparison Tables

- `rr_monthly_vectors`: RR station-year raw + normalized monthly vectors
- `ensemble_consensus_vectors`: ensemble per-file consensus vectors and
	uncertainty fields
- `ensemble_member_monthly_values`: monthly per-member values used for exact
	agreement scoring
- `similarity_sessions`: run metadata and parameters
- `similarity_matches`: ranked top-K matches per ensemble query, including
	`exact_agreement_count`
