from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import pandas as pd

from .config import ReassignConfig
from .io import assert_files_exist, ensure_parent

ID_COLS = ["gender_3", "ns_sec", "soc", "aws", "hh_type"]

NTS_MODE_TO_BT = {
    1: "WALKING",
    2: "ROAD",
    3: "ROAD",
    4: "ROAD",
    5: "ROAD",
    6: "RAIL",
    7: "SUBWAY",
}

NTS_PERIOD_TO_KEY = {
    1: "weekday_AM",
    2: "weekday_inter",
    3: "weekday_PM",
    4: "weekday_offpeak",
    5: "weekend",
    6: "weekend",
}


def _bt_period_key(pdf: pd.DataFrame) -> pd.DataFrame:
    out = pdf.copy()
    weekday_map = {
        "AM_peak": "weekday_AM",
        "Inter_peak": "weekday_inter",
        "PM_peak": "weekday_PM",
        "Off_peak": "weekday_offpeak",
    }
    out["period_key"] = out["time_period"].map(weekday_map)
    out.loc[out["weekend_flag"] == 1, "period_key"] = "weekend"
    out["period_key"] = out["period_key"].fillna("weekday_offpeak")
    out["period_key"] = out["period_key"].astype("string")
    return out


def build_msoa_purpose_shares(config: ReassignConfig) -> pd.DataFrame:
    pop_lsoa_internal = pd.read_csv(config.pop_lsoa_internal_csv)
    lsoa_cols = [c for c in pop_lsoa_internal.columns if c not in ID_COLS]
    pop_lsoa_long = pd.melt(
        pop_lsoa_internal,
        id_vars=ID_COLS,
        value_vars=lsoa_cols,
        var_name="LSOA21CD",
        value_name="pop",
    )
    pop_lsoa_long["hh_type"] = pd.to_numeric(pop_lsoa_long["hh_type"], errors="coerce")
    pop_lsoa_long["pop"] = pd.to_numeric(pop_lsoa_long["pop"], errors="coerce").fillna(0.0)

    tfn_at_lsoa = pd.read_csv(config.tfn_area_type_lsoa_csv)[["lsoa21_code", "tfn_area_type"]]
    tfn_at_lsoa = tfn_at_lsoa.rename(columns={"lsoa21_code": "LSOA21CD"})
    pop_lsoa_long = pop_lsoa_long.merge(tfn_at_lsoa, on="LSOA21CD", how="left")

    pop_lsoa_hh = (
        pop_lsoa_long.groupby(["LSOA21CD", "tfn_area_type", "hh_type"], as_index=False)["pop"].sum()
    )

    lsoa_msoa = pd.read_csv(config.lsoa_msoa_lookup_csv, usecols=["LSOA21CD", "MSOA21CD"]).drop_duplicates()
    pop_msoa_hh = (
        pop_lsoa_hh.merge(lsoa_msoa, on="LSOA21CD", how="left")
        .groupby(["MSOA21CD", "tfn_area_type", "hh_type"], as_index=False)["pop"]
        .sum()
    )

    nts_split = pd.read_csv(config.nts_mode_time_split_csv)
    nts_split["hh_type"] = pd.to_numeric(nts_split["hh_type"], errors="coerce")
    nts_split["tfn_at"] = pd.to_numeric(nts_split["tfn_at"], errors="coerce")
    nts_split["mode"] = pd.to_numeric(nts_split["mode"], errors="coerce")
    nts_split["period"] = pd.to_numeric(nts_split["period"], errors="coerce")
    nts_split["purpose"] = pd.to_numeric(nts_split["purpose"], errors="coerce")
    nts_split["rho"] = pd.to_numeric(nts_split["rho"], errors="coerce")

    nts_with_pop = nts_split.merge(
        pop_msoa_hh,
        left_on=["tfn_at", "hh_type"],
        right_on=["tfn_area_type", "hh_type"],
        how="right",
    )
    nts_with_pop = nts_with_pop.dropna(subset=["rho", "mode", "period", "purpose", "MSOA21CD"])
    nts_with_pop["trip_rho"] = nts_with_pop["rho"] * nts_with_pop["pop"]
    nts_with_pop["mode_of_transport"] = nts_with_pop["mode"].astype(int).map(NTS_MODE_TO_BT)
    nts_with_pop["period_key"] = nts_with_pop["period"].astype(int).map(NTS_PERIOD_TO_KEY)
    nts_with_pop = nts_with_pop.dropna(subset=["mode_of_transport", "period_key"])
    nts_with_pop["mode_of_transport"] = nts_with_pop["mode_of_transport"].astype("string")
    nts_with_pop["period_key"] = nts_with_pop["period_key"].astype("string")

    purpose_weights = (
        nts_with_pop.groupby(["MSOA21CD", "mode_of_transport", "period_key", "purpose"], as_index=False)["trip_rho"].sum()
    )
    purpose_weights["purpose_share"] = purpose_weights.groupby(
        ["MSOA21CD", "mode_of_transport", "period_key"]
    )["trip_rho"].transform(lambda s: s / s.sum())

    purposes = pd.read_csv(config.purposes_csv)
    purposes = purposes.rename(columns={"Purpose": "purpose", "Description": "purpose_desc"})
    purposes["purpose"] = pd.to_numeric(purposes["purpose"], errors="coerce")

    out = purpose_weights.merge(purposes[["purpose", "purpose_desc"]], on="purpose", how="left")
    out["purpose"] = out["purpose"].astype("int16")
    out["purpose_share"] = pd.to_numeric(out["purpose_share"], errors="coerce").fillna(0.0)
    out["MSOA21CD"] = out["MSOA21CD"].astype("string")
    out["mode_of_transport"] = out["mode_of_transport"].astype("string")
    out["period_key"] = out["period_key"].astype("string")
    out["purpose_desc"] = out["purpose_desc"].astype("string")
    return out[["MSOA21CD", "mode_of_transport", "period_key", "purpose", "purpose_desc", "purpose_share"]]


def run_purpose_estimation(config: ReassignConfig, adjusted_parquet: Path | None = None) -> Path:
    adjusted = adjusted_parquet or config.adjusted_parquet
    assert_files_exist(
        [
            adjusted,
            config.pop_lsoa_internal_csv,
            config.tfn_area_type_lsoa_csv,
            config.lsoa_msoa_lookup_csv,
            config.nts_mode_time_split_csv,
            config.purposes_csv,
        ]
    )
    shares = build_msoa_purpose_shares(config).rename(columns={"MSOA21CD": "origin_msoa"})
    valid_msoa = shares["origin_msoa"].dropna().astype("string").unique().tolist()

    trips_dd = dd.read_parquet(adjusted)
    if "volume_adj" in trips_dd.columns:
        trips_dd["volume_adj"] = dd.to_numeric(trips_dd["volume_adj"], errors="coerce").fillna(0.0)
    elif "volume" in trips_dd.columns:
        trips_dd["volume_adj"] = dd.to_numeric(trips_dd["volume"], errors="coerce").fillna(5.0)
    else:
        raise ValueError("Adjusted parquet must contain `volume_adj` or `volume`.")

    period_meta = trips_dd._meta.assign(period_key=pd.Series([], dtype="string"))
    trips_dd = trips_dd.map_partitions(_bt_period_key, meta=period_meta)
    trips_dd["origin_msoa"] = trips_dd["origin_msoa"].astype("string")
    trips_dd["destination_msoa"] = trips_dd["destination_msoa"].astype("string")
    trips_dd["mode_of_transport"] = trips_dd["mode_of_transport"].astype("string")
    trips_dd["period_key"] = trips_dd["period_key"].astype("string")
    # Keep purpose outputs for origins inside the MSOA universe represented by population inputs.
    # Destination can be any MSOA.
    trips_dd = trips_dd[trips_dd["origin_msoa"].isin(valid_msoa)]
    shares["origin_msoa"] = shares["origin_msoa"].astype("string")
    shares["mode_of_transport"] = shares["mode_of_transport"].astype("string")
    shares["period_key"] = shares["period_key"].astype("string")
    out_dd = trips_dd.merge(
        shares,
        on=["origin_msoa", "mode_of_transport", "period_key"],
        how="inner",
    )
    out_dd["purpose"] = out_dd["purpose"].astype("int16")
    out_dd["purpose_desc"] = out_dd["purpose_desc"].astype("string")
    out_dd["purpose_share"] = out_dd["purpose_share"].astype("float64")
    out_dd["volume_adj_purpose"] = out_dd["volume_adj"] * out_dd["purpose_share"]
    out_dd["period_key"] = out_dd["period_key"].astype("string")

    ensure_parent(config.purpose_parquet)
    out_dd.to_parquet(config.purpose_parquet)
    return config.purpose_parquet
