from pathlib import Path
from dataclasses import replace

import pandas as pd

from uk_travel_pipeline.config import ReassignConfig
from uk_travel_pipeline.purpose import build_msoa_purpose_shares, run_purpose_estimation


def _write_purpose_inputs(tmp_path: Path) -> ReassignConfig:
    pop = tmp_path / "pop_lsoa_internal.csv"
    tfn = tmp_path / "tfn_area_type_lsoa21.csv"
    lookup = tmp_path / "lsoa_msoa.csv"
    nts = tmp_path / "mode_time_split.csv"
    purposes = tmp_path / "purposes.csv"
    adjusted = tmp_path / "trips_adjusted.parquet"
    purpose_out = tmp_path / "trips_adjusted_by_purpose.parquet"

    pd.DataFrame(
        {
            "adult_nssec": [1, 2, 1],
            "gender_3": [1, 2, 1],
            "ns_sec": [1, 1, 2],
            "soc": [1, 1, 1],
            "aws": [1, 1, 1],
            "hh_type": [1, 1, 2],
            "notes": ["ignored", "ignored", "ignored"],
            "E01000001": [10.0, 5.0, 0.0],
            "W01000001": [20.0, 5.0, 10.0],
        }
    ).to_csv(pop, index=False)
    pd.DataFrame(
        {
            "lsoa21_code": ["E01000001", "W01000001"],
            "tfn_area_type": [1, 1],
        }
    ).to_csv(tfn, index=False)
    pd.DataFrame(
        {
            "LSOA21CD": ["E01000001", "W01000001"],
            "MSOA21CD": ["E02000001", "W02000001"],
        }
    ).to_csv(lookup, index=False)
    pd.DataFrame(
        {
            "hh_type": [1, 1, 2, 2],
            "tfn_at": [1, 1, 1, 1],
            "mode": [3, 3, 3, 3],
            "period": [1, 1, 1, 1],
            "purpose": [1, 2, 1, 2],
            "rho": [1.0, 3.0, 2.0, 2.0],
        }
    ).to_csv(nts, index=False)
    pd.DataFrame(
        {
            "Purpose": [1, 2],
            "Description": ["Commuting", "Business"],
        }
    ).to_csv(purposes, index=False)
    pd.DataFrame(
        {
            "origin_msoa": ["E02000001", "W02000001"],
            "destination_msoa": ["W02000001", "E02000001"],
            "mode_of_transport": ["ROAD", "ROAD"],
            "time_period": ["AM_peak", "AM_peak"],
            "weekend_flag": [0, 0],
            "days_used": [1, 1],
            "volume_adj": [100.0, 200.0],
        }
    ).to_parquet(adjusted, index=False)

    return ReassignConfig(
        adjusted_parquet=adjusted,
        purpose_parquet=purpose_out,
        pop_lsoa_internal_csv=pop,
        tfn_area_type_lsoa_csv=tfn,
        lsoa_msoa_lookup_csv=lookup,
        nts_mode_time_split_csv=nts,
        purposes_csv=purposes,
    )


def test_build_msoa_purpose_shares_handles_all_population_style_file(tmp_path: Path):
    cfg = _write_purpose_inputs(tmp_path)

    shares = build_msoa_purpose_shares(cfg)

    assert set(shares["MSOA21CD"]) == {"E02000001", "W02000001"}
    assert set(shares["mode_of_transport"]) == {"ROAD"}
    share_sums = shares.groupby(["MSOA21CD", "mode_of_transport", "period_key"])["purpose_share"].sum()
    assert all(abs(v - 1.0) < 1e-9 for v in share_sums)


def test_run_purpose_estimation_keeps_england_and_wales_origins(tmp_path: Path):
    cfg = _write_purpose_inputs(tmp_path)

    run_purpose_estimation(cfg)
    out = pd.read_parquet(cfg.purpose_parquet)

    assert set(out["origin_msoa"]) == {"E02000001", "W02000001"}
    totals = out.groupby("origin_msoa")["volume_adj_purpose"].sum().to_dict()
    assert abs(totals["E02000001"] - 100.0) < 1e-9
    assert abs(totals["W02000001"] - 200.0) < 1e-9


def test_split_road_purpose_shares_include_motorcycle_fallback(tmp_path: Path):
    cfg = replace(_write_purpose_inputs(tmp_path), split_road_mode=True)

    shares = build_msoa_purpose_shares(cfg)

    assert "MOTORCYCLE" in set(shares["mode_of_transport"])
    private_car = shares[shares["mode_of_transport"] == "PRIVATE_CAR"].sort_values(
        ["MSOA21CD", "period_key", "purpose"]
    )
    motorcycle = shares[shares["mode_of_transport"] == "MOTORCYCLE"].sort_values(
        ["MSOA21CD", "period_key", "purpose"]
    )
    assert private_car["purpose_share"].tolist() == motorcycle["purpose_share"].tolist()
