from __future__ import annotations

import argparse
from pathlib import Path

from .config import (
    DEFAULT_ADJUSTED_PARQUET,
    DEFAULT_BT_PARQUET,
    DEFAULT_EEH_LIST,
    DEFAULT_FACTOR_MAX,
    DEFAULT_FACTOR_MIN,
    DEFAULT_LEGACY_OUTPUT_ROOT,
    DEFAULT_MODES,
    DEFAULT_MSOA_GEOJSON,
    DEFAULT_NTS_CSV,
    DEFAULT_OUTPUTS_ROOT,
    DEFAULT_REGION,
    DEFAULT_YEAR,
    MatrixConfig,
    ReassignConfig,
)
from .matrix import run_matrices
from .reassign import run_reassign


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--bt-parquet", type=Path, default=DEFAULT_BT_PARQUET)
    parser.add_argument("--eeh-list", type=Path, default=DEFAULT_EEH_LIST)
    parser.add_argument("--msoa-geojson", type=Path, default=DEFAULT_MSOA_GEOJSON)
    parser.add_argument("--nts-csv", type=Path, default=DEFAULT_NTS_CSV)
    parser.add_argument("--adjusted-parquet", type=Path, default=DEFAULT_ADJUSTED_PARQUET)
    parser.add_argument("--outputs-root", type=Path, default=DEFAULT_OUTPUTS_ROOT)
    parser.add_argument("--legacy-output", action="store_true")
    parser.add_argument("--legacy-output-root", type=Path, default=DEFAULT_LEGACY_OUTPUT_ROOT)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="EEH travel model pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="Run reassignment and matrix generation")
    _add_common_args(run_parser)
    run_parser.add_argument("--year", type=int, default=DEFAULT_YEAR)
    run_parser.add_argument("--region", default=DEFAULT_REGION)
    run_parser.add_argument("--factor-min", type=float, default=DEFAULT_FACTOR_MIN)
    run_parser.add_argument("--factor-max", type=float, default=DEFAULT_FACTOR_MAX)
    run_parser.add_argument("--modes", default=",".join(DEFAULT_MODES))

    reassign_parser = sub.add_parser("reassign", help="Run reassignment stage")
    _add_common_args(reassign_parser)
    reassign_parser.add_argument("--year", type=int, default=DEFAULT_YEAR)
    reassign_parser.add_argument("--region", default=DEFAULT_REGION)
    reassign_parser.add_argument("--factor-min", type=float, default=DEFAULT_FACTOR_MIN)
    reassign_parser.add_argument("--factor-max", type=float, default=DEFAULT_FACTOR_MAX)

    matrix_parser = sub.add_parser("matrices", help="Generate OD matrices from adjusted parquet")
    _add_common_args(matrix_parser)
    matrix_parser.add_argument("--modes", default=",".join(DEFAULT_MODES))
    return parser


def _modes_from_arg(arg: str) -> tuple[str, ...]:
    items = tuple(x.strip().upper() for x in arg.split(",") if x.strip())
    if not items:
        raise ValueError("At least one mode must be provided in --modes.")
    return items


def main() -> None:
    args = build_parser().parse_args()
    legacy_root = args.legacy_output_root if args.legacy_output else None

    if args.command in {"run", "reassign"}:
        reassign_cfg = ReassignConfig(
            bt_parquet=args.bt_parquet,
            eeh_list_csv=args.eeh_list,
            msoa_geojson=args.msoa_geojson,
            nts_csv=args.nts_csv,
            adjusted_parquet=args.adjusted_parquet,
            outputs_root=args.outputs_root,
            year=args.year,
            region=args.region,
            factor_min=args.factor_min,
            factor_max=args.factor_max,
        )
        print("[reassign] starting")
        run_reassign(reassign_cfg, legacy_output_root=legacy_root)
        print(f"[reassign] wrote {reassign_cfg.adjusted_parquet}")

    if args.command in {"run", "matrices"}:
        matrix_cfg = MatrixConfig(
            adjusted_parquet=args.adjusted_parquet,
            outputs_root=args.outputs_root,
            modes=_modes_from_arg(args.modes),
        )
        print("[matrices] starting")
        run_matrices(matrix_cfg, legacy_output_root=legacy_root)
        print(f"[matrices] wrote outputs under {matrix_cfg.outputs_root / 'matrices'}")


if __name__ == "__main__":
    main()
