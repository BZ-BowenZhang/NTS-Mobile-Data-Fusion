from pathlib import Path

import pandas as pd

from uk_travel_pipeline.config import MatrixConfig
from uk_travel_pipeline.matrix import run_matrices


def test_run_matrices_outputs_files(tmp_path: Path):
    adjusted = tmp_path / "data" / "processed" / "reassign" / "trips_adjusted.parquet"
    adjusted.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "origin_msoa": ["A", "A", "B"],
            "destination_msoa": ["B", "C", "A"],
            "mode_of_transport": ["ROAD", "RAIL", "ROAD"],
            "time_period": ["AM_peak", "AM_peak", "Inter_peak"],
            "weekend_flag": [0, 0, 1],
            "days_used": [1, 1, 2],
            "volume_adj": [10, 20, 6],
        }
    )
    df.to_parquet(adjusted, index=False)

    outputs = tmp_path / "outputs"
    cfg = MatrixConfig(adjusted_parquet=adjusted, outputs_root=outputs, modes=("ROAD", "RAIL"))
    run_matrices(cfg)

    assert (outputs / "matrices" / "typical_week_by_mode" / "OD_matrix_ROAD_adjusted.csv").exists()
    assert (outputs / "matrices" / "weekday_AMpeak_by_mode" / "OD_matrix_RAIL_adjusted.csv").exists()


def test_run_matrices_outputs_purpose_files(tmp_path: Path):
    adjusted = tmp_path / "data" / "processed" / "reassign" / "trips_adjusted.parquet"
    purpose = tmp_path / "data" / "processed" / "reassign" / "trips_adjusted_by_purpose.parquet"
    adjusted.parent.mkdir(parents=True, exist_ok=True)
    base_df = pd.DataFrame(
        {
            "origin_msoa": ["A"],
            "destination_msoa": ["B"],
            "mode_of_transport": ["ROAD"],
            "time_period": ["AM_peak"],
            "weekend_flag": [0],
            "days_used": [1],
            "volume_adj": [10],
        }
    )
    purpose_df = base_df.copy()
    purpose_df["purpose"] = [1]
    purpose_df["volume_adj_purpose"] = [6.0]
    base_df.to_parquet(adjusted, index=False)
    purpose_df.to_parquet(purpose, index=False)

    outputs = tmp_path / "outputs"
    cfg = MatrixConfig(adjusted_parquet=adjusted, purpose_parquet=purpose, outputs_root=outputs, modes=("ROAD",))
    run_matrices(cfg)

    assert (outputs / "matrices" / "typical_week_by_mode" / "OD_matrix_ROAD_adjusted_by_purpose1.csv").exists()
