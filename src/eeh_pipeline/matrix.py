from __future__ import annotations

from pathlib import Path

import pandas as pd

from .config import MatrixConfig
from .io import assert_dataframe_columns, assert_files_exist, ensure_dir


ADJUSTED_REQUIRED_COLUMNS = [
    "origin_msoa",
    "destination_msoa",
    "mode_of_transport",
    "time_period",
    "weekend_flag",
    "days_used",
]


def _matrix_for_mode(df: pd.DataFrame, mode: str, value_col: str) -> pd.DataFrame:
    mode_df = df[df["mode_of_transport"] == mode]
    matrix = mode_df.pivot_table(
        index="origin_msoa",
        columns="destination_msoa",
        values=value_col,
        fill_value=0,
    )
    matrix.reset_index(inplace=True)
    return matrix


def write_matrices(df: pd.DataFrame, modes: tuple[str, ...], out_dir: Path, value_col: str) -> None:
    ensure_dir(out_dir)
    for mode in modes:
        matrix = _matrix_for_mode(df, mode=mode, value_col=value_col)
        matrix.to_csv(out_dir / f"OD_matrix_{mode}_adjusted.csv", index=False)


def run_matrices(config: MatrixConfig, legacy_output_root: Path | None = None) -> None:
    assert_files_exist([config.adjusted_parquet])
    all_data = pd.read_parquet(config.adjusted_parquet, engine="pyarrow")
    assert_dataframe_columns(all_data, ADJUSTED_REQUIRED_COLUMNS, "Adjusted parquet")

    if "volume_adj" in all_data.columns:
        all_data["volume_adj"] = pd.to_numeric(all_data["volume_adj"], errors="coerce").fillna(0).astype(int)
    elif "volume" in all_data.columns:
        all_data["volume_adj"] = pd.to_numeric(all_data["volume"], errors="coerce").fillna(5).astype(int)
    else:
        raise ValueError("Adjusted parquet must contain `volume_adj` or `volume`.")

    all_data["days_used"] = pd.to_numeric(all_data["days_used"], errors="coerce").replace(0, pd.NA)
    all_data["volume_per_day"] = (all_data["volume_adj"] / all_data["days_used"]).fillna(0)
    all_data["volume_per_typical_week"] = all_data.apply(
        lambda row: row["volume_per_day"] * 5 if row["weekend_flag"] == 0 else row["volume_per_day"] * 2,
        axis=1,
    )

    by_typical_week = all_data.groupby(
        ["origin_msoa", "destination_msoa", "mode_of_transport"], as_index=False
    )["volume_per_typical_week"].sum()

    peak_data = all_data[(all_data["weekend_flag"] == 0) & (all_data["time_period"] == "AM_peak")]
    by_am_peak = peak_data.groupby(
        ["origin_msoa", "destination_msoa", "mode_of_transport"], as_index=False
    )["volume_per_day"].sum()

    typical_dir = config.outputs_root / "matrices" / "typical_week_by_mode"
    ampeak_dir = config.outputs_root / "matrices" / "weekday_AMpeak_by_mode"
    write_matrices(by_typical_week, config.modes, typical_dir, "volume_per_typical_week")
    write_matrices(by_am_peak, config.modes, ampeak_dir, "volume_per_day")

    if legacy_output_root is not None:
        legacy_typical = legacy_output_root / "matrices" / "typical_week_by_mode"
        legacy_ampeak = legacy_output_root / "matrices" / "weekday_AMpeak_by_mode"
        write_matrices(by_typical_week, config.modes, legacy_typical, "volume_per_typical_week")
        write_matrices(by_am_peak, config.modes, legacy_ampeak, "volume_per_day")
