from __future__ import annotations

from pathlib import Path
from typing import Iterable

import dask.dataframe as dd
import pandas as pd


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def assert_files_exist(paths: Iterable[Path]) -> None:
    missing = [str(p) for p in paths if not p.exists()]
    if missing:
        raise FileNotFoundError("Missing required input files:\n" + "\n".join(missing))


def assert_dataframe_columns(df: pd.DataFrame, required: Iterable[str], name: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{name} missing required columns: {missing}")


def assert_dask_columns(df: dd.DataFrame, required: Iterable[str], name: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{name} missing required columns: {missing}")


def write_csv(df: pd.DataFrame, path: Path) -> None:
    ensure_parent(path)
    df.to_csv(path, index=False)
