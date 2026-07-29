# Export

The located, quality-controlled observations are only useful to others if they
are shared in a standard form. This stage writes them out in the
[Station Exchange Format (SEF)](https://datarescue.climate.copernicus.eu/station-exchange-format-sef),
the community standard for rescued climate data.

One notebook:

- [export_sef.ipynb](https://github.com/Philip-Brohan-MO/Auto-Daily-Rainfall-QC-MO/blob/main/notebooks/export_sef.ipynb)

## What SEF is

SEF is a simple tab-separated text format where **one file holds one variable
from one station**. Here each file is a single *station-year* of daily rainfall:
twelve header lines of metadata, a column header, then one line per observed day.

## Merging the ensemble's duplicates

The ensemble frequently contains **duplicate** transcriptions of the same
station-year (the same physical page transcribed more than once). The export
merges these — exact matches sharing a location and year — into **one SEF `.tsv`
per real station-year**, written to `<output_root>/tsv/<year>/<ID>.tsv`.

For every day the value is taken from the duplicate with the **best QC verdict**,
using this precedence so no day is dropped:

```text
qc1=pass  >  qc1=fail & qc2=pass  >  qc1=review
          >  qc1=fail & qc2=indeterminate  >  the rest
```

Each observation is the consensus daily total (the member median), **converted
from inches to millimetres**. The QC verdicts and the contributing `source=`
travel in each observation's `Meta` column (`qc1=…` from the exact-monthly check,
`qc2=…` from the secondary check), and the file-level `Meta` lists every merged
source.

## Running the export

The notebook runs a small local export into `/var/tmp`, inspects a generated
file, and validates its structure — confirming the twelve header lines are in the
required order, the data-table header is correct, every day round-trips back as a
row, and the QC verdicts are present.

The full station-year export runs as a single SLURM array — there is **no merge
stage**, because sharding by year keeps every duplicate of a station-year in the
same task and each shard writes disjoint year-partitioned files:

```bash
scripts/slurm/submit_sef_export.sh
```

Each array task exports a contiguous **matched-year** slice. Shard count, the
year range, and the SEF metadata fields (`SEF_SOURCE`, `SEF_LINK`,
`SEF_OBS_HOUR`, …) are configured in the `SEF_*` block of
`scripts/slurm/config.sh`. The prerequisites are the daily-consensus table and
the assigned ensemble metadata; the QC tables are joined when present. Files land
in `$PDIR/sef_export/tsv/<year>/<ID>.tsv` with per-shard manifests alongside.

Next: [Analysis](analysis.md).
