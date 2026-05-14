#!/usr/bin/env python3
"""Reproduce fingerprint robustness tables from the current implementation.

This script is intentionally executable outside the service stack: it imports the
seller-side watermark/fingerprint code and runs deterministic attacks against
in-memory DataFrames. It should be used to generate thesis table values, not to
format precomputed numbers.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("ALLOW_INSECURE_DEFAULTS", "true")

from src.seller.fingerprinting import (
    apply_fingerprint_to_dataframe,
    evaluate_collusion,
    generate_buyer_fingerprint,
    verify_fingerprint,
)
from src.seller._constants import DEFAULT_VERIFICATION_TOLERANCE


FOREST_CONTINUOUS_COLUMNS = [
    "Elevation",
    "Aspect",
    "Slope",
    "Horizontal_Distance_To_Hydrology",
    "Vertical_Distance_To_Hydrology",
    "Horizontal_Distance_To_Roadways",
    "Hillshade_9am",
    "Hillshade_Noon",
    "Hillshade_3pm",
    "Horizontal_Distance_To_Fire_Points",
]


@dataclass
class AttackResult:
    dataset: str
    attack: str
    detected: str
    match_rate: str
    rows: int
    note: str = ""


def make_mixed_synthetic_dataset(rows: int, seed: int) -> tuple[pd.DataFrame, list[str], str]:
    rng = np.random.default_rng(seed)
    df = pd.DataFrame(
        {
            "record_id": np.arange(rows),
            "category": rng.choice(["A", "B", "C", "D"], rows, p=[0.25, 0.3, 0.25, 0.2]),
            "write_batch": rng.integers(0, 20, rows),
            "country": rng.choice(["EE", "LV", "LT", "FI"], rows),
            "amount": rng.normal(250.0, 80.0, rows).clip(10.0, 600.0),
            "score": rng.normal(50.0, 12.0, rows).clip(0.0, 100.0),
            "quantity": rng.integers(1, 1000, rows),
            "event_time": pd.date_range("2026-01-01", periods=rows, freq="min"),
        }
    )
    return df, ["category", "write_batch"], "synthetic_mixed"


def make_forest_like_dataset(rows: int, seed: int) -> tuple[pd.DataFrame, list[str], str]:
    rng = np.random.default_rng(seed)
    df = pd.DataFrame(
        {
            "Elevation": rng.normal(2950, 280, rows).round().astype(int),
            "Aspect": rng.integers(0, 360, rows),
            "Slope": rng.integers(0, 65, rows),
            "Horizontal_Distance_To_Hydrology": rng.integers(0, 1400, rows),
            "Vertical_Distance_To_Hydrology": rng.integers(-200, 600, rows),
            "Horizontal_Distance_To_Roadways": rng.integers(0, 7000, rows),
            "Hillshade_9am": rng.integers(0, 256, rows),
            "Hillshade_Noon": rng.integers(0, 256, rows),
            "Hillshade_3pm": rng.integers(0, 256, rows),
            "Horizontal_Distance_To_Fire_Points": rng.integers(0, 7200, rows),
            "Wilderness_Area": rng.choice(["Rawah", "Neota", "Comanche", "Cache"], rows),
            "Soil_Type": rng.integers(1, 41, rows).astype(str),
            "Cover_Type": rng.integers(1, 8, rows),
        }
    )
    return df, ["Wilderness_Area", "Soil_Type"], "forest_cover_like_generated"


def load_uci_forest_cover_csv(path: Path, rows: int | None, seed: int) -> tuple[pd.DataFrame, list[str], str]:
    raw = pd.read_csv(path, header=None)
    if raw.shape[1] < 55:
        raise ValueError(
            "Expected the UCI Covertype file with 54 feature columns plus Cover_Type. "
            f"Got {raw.shape[1]} columns from {path}."
        )

    if rows is not None and rows > 0 and rows < len(raw):
        raw = raw.sample(n=rows, random_state=seed).reset_index(drop=True)

    continuous = raw.iloc[:, :10].copy()
    continuous.columns = FOREST_CONTINUOUS_COLUMNS

    wilderness_one_hot = raw.iloc[:, 10:14]
    soil_one_hot = raw.iloc[:, 14:54]
    continuous["Wilderness_Area"] = wilderness_one_hot.to_numpy().argmax(axis=1) + 1
    continuous["Soil_Type"] = soil_one_hot.to_numpy().argmax(axis=1) + 1
    continuous["Wilderness_Area"] = continuous["Wilderness_Area"].astype(str)
    continuous["Soil_Type"] = continuous["Soil_Type"].astype(str)
    continuous["Cover_Type"] = raw.iloc[:, 54].astype(int)

    return continuous, ["Wilderness_Area", "Soil_Type"], "forest_cover_type_uci"


def timestamp_columns(df: pd.DataFrame) -> list[str]:
    return [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]


def numeric_attack_columns(df: pd.DataFrame, anchor_columns: Iterable[str]) -> list[str]:
    anchors = set(anchor_columns)
    return [
        col
        for col in df.columns
        if col not in anchors
        and col != "_watermark_id"
        and pd.api.types.is_numeric_dtype(df[col])
    ]


def verify_to_result(
    dataset_name: str,
    attack_name: str,
    attacked_df: pd.DataFrame | None,
    fingerprint: dict,
    anchor_columns: list[str],
    tolerance: float,
    note: str = "",
) -> AttackResult:
    if attacked_df is None:
        return AttackResult(dataset_name, attack_name, "N/A", "N/A", 0, note)

    verification = verify_fingerprint(attacked_df, fingerprint, anchor_columns, tolerance=tolerance)
    rate = float(verification.get("overall_match_rate", 0.0))
    return AttackResult(
        dataset=dataset_name,
        attack=attack_name,
        detected="JAH" if bool(verification.get("found", False)) else "EI",
        match_rate=f"{rate:.4f}",
        rows=len(attacked_df),
        note=note,
    )


def project_anchor_and_timestamp(df: pd.DataFrame, anchor_columns: list[str]) -> pd.DataFrame | None:
    keep = [col for col in anchor_columns if col in df.columns] + timestamp_columns(df)
    if len(keep) == len([col for col in anchor_columns if col in df.columns]):
        return None
    return df[keep].copy()


def project_categorical(df: pd.DataFrame, anchor_columns: list[str]) -> pd.DataFrame:
    keep = list(dict.fromkeys([col for col in anchor_columns if col in df.columns]))
    for col in df.columns:
        if col in keep or col == "_watermark_id":
            continue
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            keep.append(col)
        elif pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            keep.append(col)
        elif isinstance(df[col].dtype, pd.CategoricalDtype):
            keep.append(col)
    return df[keep].copy()


def value_filter(df: pd.DataFrame, anchor_columns: list[str]) -> pd.DataFrame:
    for col in anchor_columns:
        if col in df.columns and df[col].nunique(dropna=True) > 1:
            mode = df[col].mode(dropna=True)
            if not mode.empty:
                filtered = df[df[col] == mode.iloc[0]].copy()
                if not filtered.empty:
                    return filtered

    numeric_cols = numeric_attack_columns(df, anchor_columns)
    if not numeric_cols:
        return df.copy()
    col = numeric_cols[0]
    return df[df[col] >= df[col].median()].copy()


def add_numeric_noise(
    df: pd.DataFrame,
    anchor_columns: list[str],
    std_fraction: float,
    seed: int,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    attacked = df.copy()
    for col in numeric_attack_columns(attacked, anchor_columns):
        std = float(attacked[col].std() or 1.0)
        attacked[col] = attacked[col] + rng.normal(0.0, std * std_fraction, len(attacked))
    return attacked


def column_aggregation(df: pd.DataFrame, anchor_columns: list[str]) -> pd.DataFrame:
    attacked = df.copy()
    numeric_cols = numeric_attack_columns(attacked, anchor_columns)
    if len(numeric_cols) < 2:
        return attacked
    attacked[numeric_cols[0]] = attacked[numeric_cols[0]] + attacked[numeric_cols[1]]
    return attacked.drop(columns=[numeric_cols[1]])


def timestamp_rounding(df: pd.DataFrame) -> pd.DataFrame | None:
    cols = timestamp_columns(df)
    if not cols:
        return None
    attacked = df.copy()
    for col in cols:
        attacked[col] = attacked[col].dt.floor("s")
    return attacked


def run_benchmark(
    source_df: pd.DataFrame,
    anchor_columns: list[str],
    dataset_name: str,
    seed: int,
    tolerance: float,
    include_trial_column: bool,
) -> list[AttackResult]:
    np.random.seed(seed)
    fingerprint = generate_buyer_fingerprint(buyer_id=101, share_id=501)
    fingerprinted = apply_fingerprint_to_dataframe(
        source_df.copy(),
        fingerprint,
        anchor_columns,
        is_trial=include_trial_column,
    )

    wrong_fingerprint = generate_buyer_fingerprint(buyer_id=999, share_id=999)
    rows: list[AttackResult] = []

    rows.append(verify_to_result(dataset_name, "Korrektne sõrmejälg (referents)", fingerprinted, fingerprint, anchor_columns, tolerance))
    rows.append(verify_to_result(dataset_name, "Vale sõrmejälg (negatiivne kontroll)", fingerprinted, wrong_fingerprint, anchor_columns, tolerance))

    rows.append(
        verify_to_result(
            dataset_name,
            "Rea kustutamine (20%)",
            fingerprinted.sample(frac=0.8, random_state=seed).copy(),
            fingerprint,
            anchor_columns,
            tolerance,
        )
    )
    rows.append(verify_to_result(dataset_name, "Väärtuse filter (WHERE)", value_filter(fingerprinted, anchor_columns), fingerprint, anchor_columns, tolerance))
    rows.append(verify_to_result(dataset_name, "Ajatempli umardamine", timestamp_rounding(fingerprinted), fingerprint, anchor_columns, tolerance, note="Ajatempli veerg puudub" if not timestamp_columns(fingerprinted) else ""))
    rows.append(verify_to_result(dataset_name, "Veerutasemel agregeerimine", column_aggregation(fingerprinted, anchor_columns), fingerprint, anchor_columns, tolerance))
    rows.append(
        verify_to_result(
            dataset_name,
            "Projektsioon (ankurveerud + ajaveerud, kui olemas)",
            project_anchor_and_timestamp(fingerprinted, anchor_columns),
            fingerprint,
            anchor_columns,
            tolerance,
            note="Aktiivset sõrmejäljekanalit ei jää alles" if project_anchor_and_timestamp(fingerprinted, anchor_columns) is None else "",
        )
    )
    rows.append(verify_to_result(dataset_name, "Kategooriaalne projektsioon", project_categorical(fingerprinted, anchor_columns), fingerprint, anchor_columns, tolerance))
    rows.append(verify_to_result(dataset_name, "Gaussi müra (5% standardhälbest)", add_numeric_noise(fingerprinted, anchor_columns, 0.05, seed + 1), fingerprint, anchor_columns, tolerance))
    rows.append(verify_to_result(dataset_name, "Perturbatsioon (1% standardhälbest)", add_numeric_noise(fingerprinted, anchor_columns, 0.01, seed + 2), fingerprint, anchor_columns, tolerance))

    for n_colluders in [2, 3, 4]:
        collusion = evaluate_collusion(
            source_df.copy(),
            anchor_columns,
            buyer_ids=list(range(201, 201 + n_colluders)),
            share_ids=list(range(701, 701 + n_colluders)),
            tolerance=tolerance,
        )
        rate = float(collusion["max_match_rate"])
        rows.append(
            AttackResult(
                dataset=dataset_name,
                attack=f"Kollusioon N={n_colluders} ({n_colluders} ostja keskmistamine)",
                detected="JAH" if rate >= tolerance else "EI",
                match_rate=f"{rate:.4f}",
                rows=len(source_df),
                note="suurim ostjapõhine vastavusmäär pärast arvuliste väärtuste keskmistamist",
            )
        )

    return rows


def to_markdown(results: list[AttackResult]) -> str:
    grouped: dict[str, list[AttackResult]] = {}
    for result in results:
        grouped.setdefault(result.dataset, []).append(result)

    lines = []
    for dataset, rows in grouped.items():
        lines.append(f"### {dataset}")
        lines.append("")
        lines.append("| Rünnak | Tuvastus | Vastavusmäär | Read | Märkus |")
        lines.append("|---|---:|---:|---:|---|")
        for row in rows:
            lines.append(f"| {row.attack} | {row.detected} | {row.match_rate} | {row.rows} | {row.note} |")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_outputs(results: list[AttackResult], output_dir: Path, basename: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = [asdict(result) for result in results]
    (output_dir / f"{basename}.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
    (output_dir / f"{basename}.md").write_text(to_markdown(results), encoding="utf-8")

    with (output_dir / f"{basename}.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=["all", "synthetic", "forest-like", "forest-uci"], default="all")
    parser.add_argument("--forest-csv", type=Path, help="Path to UCI covtype.data/csv file. Required for --dataset forest-uci.")
    parser.add_argument("--rows", type=int, default=2000, help="Rows to use or generate for each dataset.")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic random seed.")
    parser.add_argument("--tolerance", type=float, default=DEFAULT_VERIFICATION_TOLERANCE)
    parser.add_argument(
        "--include-trial-column",
        action="store_true",
        help="Also embed the trial _watermark_id channel. Default models full-share fingerprinting only.",
    )
    parser.add_argument("--output-dir", type=Path, help="Optional directory for .md, .csv and .json outputs.")
    parser.add_argument("--basename", default="fingerprint_robustness_results")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of Markdown.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    datasets: list[tuple[pd.DataFrame, list[str], str]] = []

    if args.dataset in {"all", "synthetic"}:
        datasets.append(make_mixed_synthetic_dataset(args.rows, args.seed))
    if args.dataset in {"all", "forest-like"}:
        datasets.append(make_forest_like_dataset(args.rows, args.seed + 1))
    if args.dataset == "forest-uci":
        if not args.forest_csv:
            raise SystemExit("--forest-csv is required when --dataset is forest-uci")
        datasets.append(load_uci_forest_cover_csv(args.forest_csv, args.rows, args.seed + 2))

    results: list[AttackResult] = []
    for df, anchor_columns, dataset_name in datasets:
        results.extend(
            run_benchmark(
                df,
                anchor_columns,
                dataset_name,
                seed=args.seed,
                tolerance=args.tolerance,
                include_trial_column=args.include_trial_column,
            )
        )

    if args.output_dir:
        write_outputs(results, args.output_dir, args.basename)

    if args.json:
        print(json.dumps([asdict(result) for result in results], indent=2))
    else:
        print(to_markdown(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
