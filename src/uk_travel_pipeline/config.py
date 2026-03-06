from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


DEFAULT_BT_PARQUET = Path("data/raw/bt_api/bt_modal_share_All_UK_MSOA_2024_09_2025_09.parquet")
DEFAULT_MSOA_GEOJSON = Path("data/raw/geo/Middle_layer_Super_Output_Areas_December_2021_Boundaries_EW_BGC_V3_4916445166053426.geojson")
DEFAULT_NTS_FILE = Path("data/raw/nts/nts9916.ods")
DEFAULT_MSOA_REGION_LOOKUP = Path("data/raw/lookups/msoa_to_region.csv")
DEFAULT_ADJUSTED_PARQUET = Path("data/processed/reassign/trips_adjusted.parquet")
DEFAULT_PURPOSE_PARQUET = Path("data/processed/reassign/trips_adjusted_by_purpose.parquet")
DEFAULT_OUTPUTS_ROOT = Path("outputs")
DEFAULT_LEGACY_OUTPUT_ROOT = Path("output")
DEFAULT_POP_LSOA_INTERNAL = Path("data/raw/trip_production_calculation_code/pop_lad_gb/pop_lsoa_internal.csv")
DEFAULT_TFN_AREA_TYPE_LSOA = Path("data/raw/trip_production_calculation_code/tfn_area_type_lsoa21.csv")
DEFAULT_LSOA_MSOA_LOOKUP = Path("data/raw/lookups/OA21_LAD22_LSOA21_MSOA21_LEP22_EN_LU_V2_6716459600479702985.csv")
DEFAULT_NTS_MODE_TIME_SPLIT = (
    Path("data/raw/trip_production_calculation_code/01202 - NTS Trip Rates v23.0/mode_time_split_production_hb_fr_reg.csv")
)
DEFAULT_PURPOSES_CSV = Path("data/raw/trip_production_calculation_code/01202 - NTS Trip Rates v23.0/data_definition/purposes.csv")

DEFAULT_MODES = ("ROAD", "RAIL", "WALKING", "SUBWAY")
DEFAULT_YEAR = 2024
DEFAULT_REGION = None
DEFAULT_FACTOR_MIN = 0.01
DEFAULT_FACTOR_MAX = 100.0


@dataclass(frozen=True)
class ReassignConfig:
    bt_parquet: Path = DEFAULT_BT_PARQUET
    msoa_filter_csv: Path | None = None
    msoa_region_lookup_csv: Path | None = None
    msoa_geojson: Path = DEFAULT_MSOA_GEOJSON
    nts_file: Path = DEFAULT_NTS_FILE
    adjusted_parquet: Path = DEFAULT_ADJUSTED_PARQUET
    purpose_parquet: Path = DEFAULT_PURPOSE_PARQUET
    pop_lsoa_internal_csv: Path = DEFAULT_POP_LSOA_INTERNAL
    tfn_area_type_lsoa_csv: Path = DEFAULT_TFN_AREA_TYPE_LSOA
    lsoa_msoa_lookup_csv: Path = DEFAULT_LSOA_MSOA_LOOKUP
    nts_mode_time_split_csv: Path = DEFAULT_NTS_MODE_TIME_SPLIT
    purposes_csv: Path = DEFAULT_PURPOSES_CSV
    estimate_purpose: bool = True
    outputs_root: Path = DEFAULT_OUTPUTS_ROOT
    year: int = DEFAULT_YEAR
    region: str | None = DEFAULT_REGION
    factor_min: float = DEFAULT_FACTOR_MIN
    factor_max: float = DEFAULT_FACTOR_MAX


@dataclass(frozen=True)
class MatrixConfig:
    adjusted_parquet: Path = DEFAULT_ADJUSTED_PARQUET
    purpose_parquet: Path | None = DEFAULT_PURPOSE_PARQUET
    outputs_root: Path = DEFAULT_OUTPUTS_ROOT
    modes: tuple[str, ...] = DEFAULT_MODES


@dataclass(frozen=True)
class PipelineConfig:
    reassign: ReassignConfig
    matrix: MatrixConfig
    legacy_output: bool = False
    legacy_output_root: Path = DEFAULT_LEGACY_OUTPUT_ROOT
