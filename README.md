# EEH Pipeline

Standard Python project for converting the notebook workflow into a reproducible CLI pipeline.

## Directory Contract

- Code: `src/eeh_pipeline`
- Raw immutable inputs: `data/raw`
- Processed intermediates: `data/processed`
- Final outputs: `outputs`
- Optional legacy compatibility outputs: `output` (enabled with `--legacy-output`)

## Expected Raw Inputs

- `data/raw/bt_api/bt_modal_share_All_UK_MSOA_2024_09_2025_09.parquet`
- `data/raw/lookups/EEH_MSOACDs.csv`
- `data/raw/geo/msoa_2021_boundaries.geojson`
- `data/raw/nts/nts9916.csv`

This repo currently provides symlinks from these paths to your existing source files.

## Install

```bash
pip install -e .
```

For tests:

```bash
pip install -e .[dev]
```

## CLI

Run full pipeline:

```bash
eeh-pipeline run
```

Run only reassignment:

```bash
eeh-pipeline reassign
```

Run only matrix generation:

```bash
eeh-pipeline matrices
```

Optional legacy outputs:

```bash
eeh-pipeline run --legacy-output
```

## Main Outputs

- `data/processed/reassign/EEH_trips_adjusted.parquet`
- `outputs/reassign/adjustment_factors.csv`
- `outputs/reassign/share_check.csv`
- `outputs/matrices/typical_week_by_mode/OD_matrix_{MODE}_adjusted.csv`
- `outputs/matrices/weekday_AMpeak_by_mode/OD_matrix_{MODE}_adjusted.csv`

## Migration Notes

- Notebook logic from `reassign_travel_model.ipynb` and `generate_od_matrices.ipynb` is now implemented in package modules and CLI stages.
- Safe fix applied: matrix stage uses `volume_adj` preferentially when deriving per-day and weekly values.
- Previous `output/...` paths are written only when `--legacy-output` is enabled.
