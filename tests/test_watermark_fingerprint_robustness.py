import os
import sys
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("ALLOW_INSECURE_DEFAULTS", "true")

from src.seller.watermarking import generate_watermark, apply_watermark_to_dataframe
from src.utils.secrets_utils import generate_nonce
from src.seller.fingerprinting import generate_buyer_fingerprint, verify_fingerprint, evaluate_robustness, compute_false_positive_rate

def _make_test_df(n=200):
    np.random.seed(42)
    return pd.DataFrame({
        "id": range(n),
        "category": np.random.choice(["A", "B", "C"], n),
        "write_batch": np.random.randint(0, 10, n),
        "amount": np.random.uniform(10, 1000, n),
        "timestamp": pd.date_range("2024-01-01", periods=n, freq="h"),
        "score": np.random.normal(50, 10, n),
    })

ANCHOR_COLUMNS = ["category", "write_batch"]

def test_watermark_survival():
    df = _make_test_df()
    watermark = generate_watermark(seller_id=1, dataset_id=1)
    wdf = apply_watermark_to_dataframe(df.copy(), watermark, anchor_columns=ANCHOR_COLUMNS)

    assert len(wdf) >= len(df), "Watermarked df should have at least as many rows"
    print("[OK] test_watermark_survival")

def test_degradation_under_perturbation():
    df = _make_test_df()
    watermark = generate_watermark(seller_id=2, dataset_id=2)
    wdf = apply_watermark_to_dataframe(df.copy(), watermark, anchor_columns=ANCHOR_COLUMNS)

    perturbed = wdf.copy()
    for col in ["amount", "score"]:
        if col in perturbed.columns:
            noise = np.random.normal(0, 0.01, len(perturbed))
            perturbed[col] = perturbed[col] + noise

    print("[OK] test_degradation_under_perturbation")

def test_fingerprint_robustness_profile():
    df = _make_test_df()
    fingerprint = generate_buyer_fingerprint(buyer_id=3, share_id=3)
    from src.seller.fingerprinting import apply_fingerprint_to_dataframe
    fdf = apply_fingerprint_to_dataframe(df.copy(), fingerprint, ANCHOR_COLUMNS, is_trial=True)

    robustness = evaluate_robustness(fdf, fingerprint, ANCHOR_COLUMNS)

    for attack in ["row_deletion", "projection", "row_reordering", "value_filter"]:
        if attack in robustness["attacks"]:
            r = robustness["attacks"][attack]
            assert r["survived"], f"{attack} should survive, got match_rate={r['match_rate']:.4f}"
            assert r["match_rate"] >= 0.5, f"{attack} match_rate should be >= 0.5, got {r['match_rate']:.4f}"
            print(f"  {attack}: survived={r['survived']}, rate={r['match_rate']:.4f}")

    for attack in ["perturbation", "numeric_noise"]:
        if attack in robustness["attacks"]:
            r = robustness["attacks"][attack]
            assert not r["survived"], f"{attack} should NOT survive, got match_rate={r['match_rate']:.4f}"
            assert r["match_rate"] < 0.3, f"{attack} match_rate should be < 0.3, got {r['match_rate']:.4f}"
            print(f"  {attack}: survived={r['survived']}, rate={r['match_rate']:.4f}")

    print("[OK] test_fingerprint_robustness_profile")

def test_value_filter_survival():
    df = _make_test_df(500)
    fingerprint = generate_buyer_fingerprint(buyer_id=4, share_id=4)
    from src.seller.fingerprinting import apply_fingerprint_to_dataframe
    fdf = apply_fingerprint_to_dataframe(df.copy(), fingerprint, ANCHOR_COLUMNS, is_trial=True)

    filtered = fdf[fdf["category"] == "A"].copy()
    assert len(filtered) > 0, "Filtered df should have rows"

    result = verify_fingerprint(filtered, fingerprint, ANCHOR_COLUMNS)
    print(f"  Filtered to {len(filtered)} rows, match_rate={result.get('overall_match_rate', 0):.4f}")
    assert result.get("overall_match_rate", 0) >= 0.3, f"Fingerprint should survive filtering, got {result}"
    print("[OK] test_value_filter_survival")

def test_timestamp_rounding_degradation():
    df = _make_test_df()
    fingerprint = generate_buyer_fingerprint(buyer_id=5, share_id=5)
    from src.seller.fingerprinting import apply_fingerprint_to_dataframe
    fdf = apply_fingerprint_to_dataframe(df.copy(), fingerprint, ANCHOR_COLUMNS, is_trial=True)

    rounded = fdf.copy()
    if "timestamp" in rounded.columns:
        rounded["timestamp"] = rounded["timestamp"].dt.floor("s")

    result = verify_fingerprint(rounded, fingerprint, ANCHOR_COLUMNS)
    rate = result.get("overall_match_rate", 0)
    print(f"  After rounding: match_rate={rate:.4f}")
    assert rate >= 0.0, "Match rate should be non-negative"
    print("[OK] test_timestamp_rounding_degradation")

def test_aggregation_destruction():
    df = _make_test_df()
    fingerprint = generate_buyer_fingerprint(buyer_id=6, share_id=6)
    from src.seller.fingerprinting import apply_fingerprint_to_dataframe
    fdf = apply_fingerprint_to_dataframe(df.copy(), fingerprint, ANCHOR_COLUMNS, is_trial=True)

    agg = fdf.groupby("category").agg({"amount": "mean", "score": "mean"}).reset_index()
    assert len(agg) < len(fdf), "Aggregation should reduce row count"

    result = verify_fingerprint(agg, fingerprint, ANCHOR_COLUMNS)
    rate = result.get("overall_match_rate", 0)
    print(f"  After aggregation: {len(agg)} rows, match_rate={rate:.4f}")
    assert rate < 0.3, f"Fingerprint should not survive aggregation, got {rate:.4f}"
    print("[OK] test_aggregation_destruction")

def test_strong_noise_destruction():
    df = _make_test_df()
    watermark = generate_watermark(seller_id=7, dataset_id=7)
    wdf = apply_watermark_to_dataframe(df.copy(), watermark, anchor_columns=ANCHOR_COLUMNS)

    noisy = wdf.copy()
    for col in ["amount", "score"]:
        if col in noisy.columns:
            noisy[col] = np.random.uniform(0, 1000, len(noisy))
    print("[OK] test_strong_noise_destruction")

def test_false_positive_rate():
    result = compute_false_positive_rate(anchor_columns=ANCHOR_COLUMNS, num_trials=100, num_rows=100)
    fpr = result["false_positive_rate"]
    print(f"  False positive rate: {fpr:.4f} ({result['false_positives']}/{result['total_trials']})")
    assert fpr < 0.05, f"False positive rate too high: {fpr}"
    print("[OK] test_false_positive_rate")

def test_statistical_watermark_verification():
    df = _make_test_df(1000)
    nonce = generate_nonce()
    watermark = generate_watermark(seller_id=100, dataset_id=100, nonce=nonce)
    wdf = apply_watermark_to_dataframe(df.copy(), watermark, anchor_columns=ANCHOR_COLUMNS)

    original_amounts = df["amount"].values
    watermarked_amounts = wdf["amount"].values[:len(original_amounts)]

    diffs = np.abs(original_amounts - watermarked_amounts)
    modified_count = np.sum(diffs > 0)
    print(f"  Modified {modified_count}/{len(original_amounts)} amount values")
    print(f"  Mean diff: {np.mean(diffs):.6f}")
    print("[OK] test_statistical_watermark_verification")

if __name__ == "__main__":
    test_watermark_survival()
    test_degradation_under_perturbation()
    test_fingerprint_robustness_profile()
    test_value_filter_survival()
    test_timestamp_rounding_degradation()
    test_aggregation_destruction()
    test_strong_noise_destruction()
    test_false_positive_rate()
    test_statistical_watermark_verification()
    print("All watermark/fingerprint robustness tests passed!")
