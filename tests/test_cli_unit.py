from pathlib import Path

from uk_travel_pipeline.cli import build_parser


def test_cli_supports_explicit_api_and_filter_paths():
    parser = build_parser()
    args = parser.parse_args(
        [
            "run",
            "--api-data-path",
            "custom/api_data.parquet",
            "--msoa-filter-list-path",
            "custom/filter.csv",
        ]
    )
    assert args.bt_parquet == Path("custom/api_data.parquet")
    assert args.msoa_filter_list == Path("custom/filter.csv")


def test_cli_keeps_legacy_aliases_for_paths():
    parser = build_parser()
    args = parser.parse_args(
        [
            "run",
            "--bt-parquet",
            "legacy/api_data.parquet",
            "--msoa-filter-list",
            "legacy/filter.csv",
        ]
    )
    assert args.bt_parquet == Path("legacy/api_data.parquet")
    assert args.msoa_filter_list == Path("legacy/filter.csv")
