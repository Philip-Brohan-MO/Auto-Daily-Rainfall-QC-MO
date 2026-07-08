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
