from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import geopandas as gpd
import numpy as np
import pandas as pd

from .config import ReassignConfig
from .io import assert_dataframe_columns, assert_dask_columns, assert_files_exist, ensure_dir, ensure_parent, write_csv

MILE_IN_M = 1609.344

BT_REQUIRED_COLUMNS = [
    "origin_msoa",
    "destination_msoa",
    "volume",
    "days_used",
    "journey_time_mean",
    "mode_of_transport",
    "time_period",
    "weekend_flag",
]

NTS_MODE_COLS = [
    "Walk",
    "Pedal cycle",
    "Car or van driver",
    "Car or van passenger",
    "Motorcycle",
    "Other private transport",
    "Bus in London",
    "Other local bus",
    "Non-local bus",
    "London Underground",
    "Surface Rail",
    "Taxi or minicab",
    "Other public transport ",
]

NTS_TO_BT = {
    "Walk": "WALKING",
    "Pedal cycle": "ROAD",
    "Car or van driver": "ROAD",
    "Car or van passenger": "ROAD",
    "Motorcycle": "ROAD",
    "Other private transport": "ROAD",
    "Bus in London": "ROAD",
    "Other local bus": "ROAD",
    "Non-local bus": "ROAD",
    "Taxi or minicab": "ROAD",
    "London Underground": "SUBWAY",
    "Surface Rail": "RAIL",
    "Other public transport ": "RAIL",
}


def _add_distances(pdf: pd.DataFrame) -> pd.DataFrame:
    dx = pdf["ox"].to_numpy() - pdf["dx"].to_numpy()
    dy = pdf["oy"].to_numpy() - pdf["dy"].to_numpy()
    pdf["distance_m"] = np.sqrt(dx * dx + dy * dy).astype("float32")
    pdf["distance_miles"] = (pdf["distance_m"] / MILE_IN_M).astype("float32")
    return pdf


def add_distance_bands(trips_dd: dd.DataFrame, labels: list[str]) -> dd.DataFrame:
    bins = [0, 1, 2, 5, 10, 25, 50, 100, np.inf]

    def _add_band(pdf: pd.DataFrame) -> pd.DataFrame:
        pdf["distance_band"] = pd.cut(
            pdf["distance_miles"],
            bins=bins,
            labels=labels,
            right=False,
            include_lowest=True,
        )
        return pdf

    meta = trips_dd._meta.assign(
        distance_band=pd.Categorical(pd.Series([], dtype="object"), categories=labels, ordered=True)
    )
    return trips_dd.map_partitions(_add_band, meta=meta)


def build_nts_mode_shares(nts_df: pd.DataFrame, year: int, region: str) -> pd.DataFrame:
    required = ["Year", "Trip length", "Region of residence"] + NTS_MODE_COLS
    assert_dataframe_columns(nts_df, required, "NTS csv")

    nts = nts_df.copy()
    for col in NTS_MODE_COLS:
        nts[col] = pd.to_numeric(nts[col], errors="coerce")

    nts = nts[nts["Trip length"].ne("All lengths")].copy()
    nts_long = nts.melt(
        id_vars=["Year", "Trip length", "Region of residence"],
        value_vars=NTS_MODE_COLS,
        var_name="nts_mode",
        value_name="nts_trips_thousands",
    )
    nts_long["bt_mode"] = nts_long["nts_mode"].map(NTS_TO_BT)

    nts_bt = (
        nts_long.dropna(subset=["bt_mode"])
        .groupby(["Year", "Region of residence", "Trip length", "bt_mode"], as_index=False)["nts_trips_thousands"]
        .sum()
    )

    nts_band_mode = nts_bt[(nts_bt["Year"] == year) & (nts_bt["Region of residence"] == region)].copy()
    nts_band_mode = nts_band_mode.rename(columns={"Trip length": "distance_band"})
    nts_band_mode["nts_share"] = nts_band_mode.groupby("distance_band")["nts_trips_thousands"].transform(
        lambda s: s / s.sum()
    )
    return nts_band_mode[["distance_band", "bt_mode", "nts_share"]]


def calculate_factors(trips_dd: dd.DataFrame, nts_band_mode: pd.DataFrame, factor_min: float, factor_max: float) -> pd.DataFrame:
    bt_band_mode = (
        trips_dd.groupby(["distance_band", "mode_of_transport"])["volume"]
        .sum()
        .compute()
        .reset_index()
        .rename(columns={"mode_of_transport": "bt_mode", "volume": "bt_volume"})
    )

    bt_band_mode["bt_share"] = bt_band_mode.groupby("distance_band")["bt_volume"].transform(lambda s: s / s.sum())

    factors = bt_band_mode.merge(nts_band_mode, on=["distance_band", "bt_mode"], how="left")
    factors["nts_share"] = factors["nts_share"].fillna(factors["bt_share"])
    eps = 1e-12
    factors["factor"] = factors["nts_share"] / (factors["bt_share"] + eps)
    factors["factor"] = factors["factor"].clip(factor_min, factor_max)
    return factors[["distance_band", "bt_mode", "factor"]]


def run_reassign(config: ReassignConfig, legacy_output_root: Path | None = None) -> Path:
    assert_files_exist([config.bt_parquet, config.eeh_list_csv, config.msoa_geojson, config.nts_csv])

    all_trips_dd = dd.read_parquet(config.bt_parquet, columns=BT_REQUIRED_COLUMNS)
    assert_dask_columns(all_trips_dd, BT_REQUIRED_COLUMNS, "BT parquet")

    eeh_list = pd.read_csv(config.eeh_list_csv)
    assert_dataframe_columns(eeh_list, ["MSOA21CD"], "EEH list csv")
    eeh_set = set(eeh_list["MSOA21CD"].astype(str))

    eeh_trips_dd = all_trips_dd[
        all_trips_dd["origin_msoa"].isin(eeh_set) & all_trips_dd["destination_msoa"].isin(eeh_set)
    ]

    msoa = gpd.read_file(config.msoa_geojson).to_crs(epsg=27700)
    assert_dataframe_columns(msoa, ["MSOA21CD", "geometry"], "MSOA geojson")
    msoa = msoa[msoa["MSOA21CD"].astype(str).isin(eeh_set)].copy()
    msoa["centroid"] = msoa.geometry.centroid
    cent = pd.DataFrame(
        {
            "MSOA21CD": msoa["MSOA21CD"].astype(str).values,
            "x": msoa["centroid"].x.values.astype("float32"),
            "y": msoa["centroid"].y.values.astype("float32"),
        }
    )

    cent_o = cent.rename(columns={"MSOA21CD": "origin_msoa", "x": "ox", "y": "oy"})
    cent_d = cent.rename(columns={"MSOA21CD": "destination_msoa", "x": "dx", "y": "dy"})
    eeh_trips_dd = eeh_trips_dd.merge(cent_o, on="origin_msoa", how="left")
    eeh_trips_dd = eeh_trips_dd.merge(cent_d, on="destination_msoa", how="left")

    meta = eeh_trips_dd._meta.assign(distance_m=np.float32(), distance_miles=np.float32())
    eeh_trips_dd = eeh_trips_dd.map_partitions(_add_distances, meta=meta)
    eeh_trips_dd = eeh_trips_dd.drop(columns=["ox", "oy", "dx", "dy"])

    eeh_trips_dd["volume"] = dd.to_numeric(eeh_trips_dd["volume"], errors="coerce").fillna(5).astype("int32")

    nts_df = pd.read_csv(config.nts_csv)
    labels = [x for x in nts_df["Trip length"].dropna().unique().tolist() if x != "All lengths"]
    eeh_trips_dd = add_distance_bands(eeh_trips_dd, labels)

    nts_band_mode = build_nts_mode_shares(nts_df, year=config.year, region=config.region)
    factors = calculate_factors(eeh_trips_dd, nts_band_mode, config.factor_min, config.factor_max)

    eeh_trips_dd = eeh_trips_dd.merge(
        factors.rename(columns={"bt_mode": "mode_of_transport"}),
        on=["distance_band", "mode_of_transport"],
        how="left",
    )
    eeh_trips_dd["factor"] = eeh_trips_dd["factor"].fillna(1.0)
    eeh_trips_dd["volume_adj"] = (eeh_trips_dd["volume"] * eeh_trips_dd["factor"]).round().astype("int64")
    eeh_trips_dd = eeh_trips_dd.rename(columns={"factor": "adj_factor"})

    check = (
        eeh_trips_dd.groupby(["distance_band", "mode_of_transport"])[["volume", "volume_adj"]]
        .sum()
        .compute()
        .reset_index()
    )
    check["bt_share_before"] = check.groupby("distance_band")["volume"].transform(lambda s: s / s.sum())
    check["bt_share_after"] = check.groupby("distance_band")["volume_adj"].transform(lambda s: s / s.sum())
    check = check.rename(columns={"mode_of_transport": "bt_mode"})
    check = check.merge(nts_band_mode, on=["distance_band", "bt_mode"], how="left")

    reassign_out = config.outputs_root / "reassign"
    ensure_dir(reassign_out)
    write_csv(factors.sort_values(["distance_band", "bt_mode"]), reassign_out / "adjustment_factors.csv")
    write_csv(
        check[
            ["distance_band", "bt_mode", "volume", "volume_adj", "bt_share_before", "bt_share_after", "nts_share"]
        ].sort_values(["distance_band", "bt_mode"]),
        reassign_out / "share_check.csv",
    )

    ensure_parent(config.adjusted_parquet)
    eeh_trips_dd.drop(columns=["distance_m", "distance_band"]).to_parquet(config.adjusted_parquet)

    if legacy_output_root is not None:
        legacy_out = legacy_output_root / "EEH_trips_adjusted.parquet"
        ensure_parent(legacy_out)
        eeh_trips_dd.drop(columns=["distance_m", "distance_band"]).to_parquet(legacy_out)

    return config.adjusted_parquet
