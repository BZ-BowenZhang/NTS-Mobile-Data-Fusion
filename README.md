# UK Travel Pipeline

Standard Python project for converting the notebook workflow into a reproducible CLI pipeline.

## Directory Contract

- Code: `src/uk_travel_pipeline`
- Raw immutable inputs: `data/raw`
- Processed intermediates: `data/processed`
- Final outputs: `outputs`
- Optional legacy compatibility outputs: `output` (enabled with `--legacy-output`)

## Expected Raw Inputs

- `data/raw/bt_api/bt_modal_share_All_UK_MSOA_2024_09_2025_09.parquet`
- `data/raw/geo/msoa_2021_boundaries.geojson`
- `data/raw/nts/nts9916.ods` (table `NTS9916a_trips_region` is parsed automatically)
- Optional MSOA filter list: `data/raw/lookups/region_MSOACDs.csv`
- Optional MSOA-to-region lookup: `data/raw/lookups/msoa_to_region.csv` with columns `MSOA21CD` and `Region of residence`
  - If this file exists at the default path above, CLI will auto-use it.

This repo currently provides symlinks from these paths to your existing source files.

## Install

```bash
python -m pip install -e .
```

For tests:

```bash
python -m pip install -e .[dev]
```

## CLI

Run full pipeline:

```bash
uk-travel-pipeline run
```

By default, reassignment runs on all UK MSOAs in the BT parquet.
To filter to a specific region, pass an MSOA list explicitly:

```bash
uk-travel-pipeline run \
  --api-data-path data/raw/bt_api/bt_modal_share_All_UK_MSOA_2024_09_2025_09.parquet \
  --msoa-filter-list-path data/raw/lookups/region_MSOACDs.csv \
  --msoa-region-lookup-path data/raw/lookups/msoa_to_region.csv \
  --nts-file data/raw/nts/nts9916.ods \
  --year 2024
```

The reassignment stage now matches NTS shares by:
- selected `--year` (including `YYYY to YYYY` rows in NTS)
- the NTS region of each `origin_msoa`

For v0.2.0, reassignment also automatically estimates `volume_adj` by purpose using:
- `pop_lad_gb/pop_lsoa_internal.csv`
- `tfn_area_type_lsoa21.csv`
- `OA21_LAD22_LSOA21_MSOA21...csv`
- `mode_time_split_production_hb_fr_reg.csv`

This writes:
- `data/processed/reassign/trips_adjusted_by_purpose.parquet`

Disable with:

```bash
uk-travel-pipeline run --skip-purpose-estimation
```

Optional v0.3.0 feature:

```bash
uk-travel-pipeline run --split-road-mode
```

This replaces `ROAD` with four NTS-consolidated submodes:
- `CYCLE`
- `PRIVATE_CAR`
- `MOTORCYCLE`
- `BUS`

## Build `msoa_to_region.csv`

If you have an official lookup file that already contains both `MSOA21CD` and a region name column
(for example `Region of residence` or `RGN21NM`):

```bash
python scripts/build_msoa_to_region_lookup.py \
  --msoa-lookup-csv /path/to/official_msoa_lookup.csv \
  --output-csv data/raw/lookups/msoa_to_region.csv
```

If your MSOA lookup has LAD codes only (for example `LAD22CD`), pass a second LAD-to-region lookup:

```bash
python scripts/build_msoa_to_region_lookup.py \
  --msoa-lookup-csv /path/to/msoa_to_lad_lookup.csv \
  --lad-region-lookup-csv /path/to/lad_to_region_lookup.csv \
  --output-csv data/raw/lookups/msoa_to_region.csv
```

Using your downloaded official files directly:

```bash
python scripts/build_msoa_to_region_lookup.py \
  --msoa-lookup-csv "/Users/bowenzhang/Downloads/MSOA_(2011)_to_MSOA_(2021)_to_Local_Authority_District_(2022)_Exact_Fit_Lookup_for_EW_(V2).csv" \
  --lad-region-lookup-csv "/Users/bowenzhang/Downloads/lasregionew2021lookup.xlsx" \
  --output-csv data/raw/lookups/msoa_to_region.csv
```

Run only reassignment:

```bash
uk-travel-pipeline reassign
```

Run only matrix generation:

```bash
uk-travel-pipeline matrices
```

Optional legacy outputs:

```bash
uk-travel-pipeline run --legacy-output
```

## Main Outputs

- `data/processed/reassign/trips_adjusted.parquet`
- `outputs/reassign/adjustment_factors.csv`
- `outputs/reassign/share_check.csv`
- `outputs/matrices/typical_week_by_mode/OD_matrix_{MODE}_adjusted.csv`
- `outputs/matrices/weekday_AMpeak_by_mode/OD_matrix_{MODE}_adjusted.csv`
- `outputs/matrices/typical_week_by_mode/OD_matrix_{MODE}_adjusted_by_purpose{X}.csv`
- `outputs/matrices/weekday_AMpeak_by_mode/OD_matrix_{MODE}_adjusted_by_purpose{X}.csv`
