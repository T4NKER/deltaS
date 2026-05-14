#!/usr/bin/env python3
"""Evaluate the prototype's heuristic PII detector on controlled data.

The script is intentionally independent of the Docker stack. It imports the
seller-side PII detector and computes column-level precision/recall for a small
deterministic dataset with known PII and clean columns.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("ALLOW_INSECURE_DEFAULTS", "true")

from src.seller.pii_detection import analyze_dataset_for_pii, assess_privacy_risk


@dataclass
class EvaluationResult:
    dataset: str
    rows: int
    expected_pii_columns: list[str]
    expected_clean_columns: list[str]
    detected_pii_columns: list[str]
    true_positives: int
    false_positives: int
    false_negatives: int
    true_negatives: int
    precision: float
    recall: float
    specificity: float
    f1: float
    risk_score: float
    risk_level: str
    privacy_status: str
    pii_types: dict[str, int]
    sensitive_columns: dict[str, list[str]]


def make_controlled_pii_dataset() -> tuple[pd.DataFrame, set[str], set[str]]:
    def make_isikukood(first_ten_digits: str) -> str:
        digits = [int(d) for d in first_ten_digits]
        weights1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 1]
        rem = sum(d * w for d, w in zip(digits, weights1)) % 11
        if rem < 10:
            return first_ten_digits + str(rem)
        weights2 = [3, 4, 5, 6, 7, 8, 9, 1, 2, 3]
        rem = sum(d * w for d, w in zip(digits, weights2)) % 11
        return first_ten_digits + (str(rem) if rem < 10 else "0")

    df = pd.DataFrame(
        {
            "email": [
                "alice@example.com",
                "bob@example.org",
                "carol@example.net",
                "dave@example.ee",
                "eve@example.com",
                "frank@example.org",
                "grace@example.net",
                "heidi@example.ee",
            ],
            "isikukood": [
                make_isikukood("3800108571"),
                make_isikukood("4730704823"),
                make_isikukood("6050918000"),
                make_isikukood("3691220002"),
                make_isikukood("4980108492"),
                make_isikukood("3840427601"),
                make_isikukood("4710101003"),
                make_isikukood("6040705601"),
            ],
            "phone": [
                "+37251234567",
                "+37253456789",
                "+37255667788",
                "+3725012345",
                "+37255500011",
                "+37255500022",
                "+37255500033",
                "+37255500044",
            ],
            "ip_address": [
                "192.168.1.10",
                "10.0.0.12",
                "172.16.5.20",
                "8.8.8.8",
                "1.1.1.1",
                "203.0.113.8",
                "198.51.100.4",
                "192.0.2.15",
            ],
            "record_id": list(range(1, 9)),
            "amount": [12.5, 33.0, 41.2, 18.9, 77.1, 91.3, 25.0, 64.8],
            "category": ["A", "B", "A", "C", "B", "A", "C", "B"],
            "comment": [
                "ordinary row",
                "clean sample",
                "no identifier",
                "business metric",
                "synthetic note",
                "safe value",
                "plain text",
                "control value",
            ],
        }
    )
    expected_pii = {"email", "isikukood", "phone", "ip_address"}
    expected_clean = {"record_id", "amount", "category", "comment"}
    return df, expected_pii, expected_clean


def make_numeric_control_dataset(rows: int = 2000, seed: int = 42) -> tuple[pd.DataFrame, set[str], set[str]]:
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
            "Wilderness_Area": rng.integers(1, 5, rows),
            "Soil_Type": rng.integers(1, 41, rows),
            "Cover_Type": rng.integers(1, 8, rows),
        }
    )
    return df, set(), set(df.columns)


def load_uci_forest_cover_csv(path: Path, rows: int = 2000, seed: int = 42) -> tuple[pd.DataFrame, set[str], set[str]]:
    raw = pd.read_csv(path, header=None)
    if raw.shape[1] < 55:
        raise ValueError(f"Expected UCI Covertype format with at least 55 columns, got {raw.shape[1]}")
    if rows and rows < len(raw):
        raw = raw.sample(n=rows, random_state=seed).reset_index(drop=True)

    continuous_columns = [
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
    df = raw.iloc[:, :10].copy()
    df.columns = continuous_columns
    df["Wilderness_Area"] = raw.iloc[:, 10:14].to_numpy().argmax(axis=1) + 1
    df["Soil_Type"] = raw.iloc[:, 14:54].to_numpy().argmax(axis=1) + 1
    df["Cover_Type"] = raw.iloc[:, 54].astype(int)
    return df, set(), set(df.columns)


def evaluate(df: pd.DataFrame, expected_pii: set[str], expected_clean: set[str], dataset_name: str) -> EvaluationResult:
    sensitive_columns, pii_types, risk_score, risk_level = analyze_dataset_for_pii(df)
    privacy = assess_privacy_risk(sensitive_columns, pii_types, risk_score)

    detected = set(sensitive_columns.keys())
    tp = len(detected & expected_pii)
    fp = len(detected & expected_clean)
    fn = len(expected_pii - detected)
    tn = len(expected_clean - detected)

    precision = tp / (tp + fp) if (tp + fp) else 1.0
    recall = tp / (tp + fn) if (tp + fn) else 1.0
    specificity = tn / (tn + fp) if (tn + fp) else 1.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0

    return EvaluationResult(
        dataset=dataset_name,
        rows=len(df),
        expected_pii_columns=sorted(expected_pii),
        expected_clean_columns=sorted(expected_clean),
        detected_pii_columns=sorted(detected),
        true_positives=tp,
        false_positives=fp,
        false_negatives=fn,
        true_negatives=tn,
        precision=precision,
        recall=recall,
        specificity=specificity,
        f1=f1,
        risk_score=float(risk_score),
        risk_level=str(risk_level),
        privacy_status=str(privacy.get("status")),
        pii_types={str(k): int(v) for k, v in pii_types.items()},
        sensitive_columns={str(k): [str(v) for v in vals] for k, vals in sensitive_columns.items()},
    )


def print_result(result: EvaluationResult) -> None:
    print(f"\nDataset: {result.dataset}")
    print(f"Rows: {result.rows}")
    print(f"Expected PII columns: {', '.join(result.expected_pii_columns) or '-'}")
    print(f"Detected PII columns: {', '.join(result.detected_pii_columns) or '-'}")
    print(
        "Confusion matrix: "
        f"TP={result.true_positives}, FP={result.false_positives}, "
        f"FN={result.false_negatives}, TN={result.true_negatives}"
    )
    print(
        "Metrics: "
        f"precision={result.precision:.4f}, recall={result.recall:.4f}, "
        f"specificity={result.specificity:.4f}, f1={result.f1:.4f}"
    )
    print(f"Privacy status: {result.privacy_status}; risk={result.risk_score:.4f} ({result.risk_level})")
    print(f"PII types: {json.dumps(result.pii_types, sort_keys=True)}")
    print(f"Sensitive columns: {json.dumps(result.sensitive_columns, sort_keys=True)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate PII detector column-level precision and recall.")
    parser.add_argument("--json-out", type=Path, help="Optional path for JSON results.")
    parser.add_argument("--rows", type=int, default=2000, help="Rows for numeric control dataset.")
    parser.add_argument("--forest-csv", type=Path, help="Optional UCI Covertype CSV/data file for numeric control.")
    args = parser.parse_args()

    controlled_df, controlled_pii, controlled_clean = make_controlled_pii_dataset()
    if args.forest_csv:
        numeric_df, numeric_pii, numeric_clean = load_uci_forest_cover_csv(args.forest_csv, rows=args.rows)
        numeric_name = "forest_cover_type_uci_control"
    else:
        numeric_df, numeric_pii, numeric_clean = make_numeric_control_dataset(rows=args.rows)
        numeric_name = "numeric_forest_cover_type_like_control"

    results = [
        evaluate(controlled_df, controlled_pii, controlled_clean, "controlled_pii_synthetic"),
        evaluate(numeric_df, numeric_pii, numeric_clean, numeric_name),
    ]

    for result in results:
        print_result(result)

    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(
            json.dumps([asdict(r) for r in results], indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"\nWrote JSON results to {args.json_out}")

    failures = [r for r in results if r.false_positives or r.false_negatives]
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
