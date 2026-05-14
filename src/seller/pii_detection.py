import re
import pandas as pd
import phonenumbers
from typing import List, Dict, Tuple
from collections import Counter

PII_PATTERNS = {
    'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
    'isikukood': r'\b[1-6]\d{10}\b',
    'credit_card': r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b',
    'ip_address': r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
    'url': r'https?://[^\s]+',
}

_PATTERN_CANONICAL = {}

DIRECT_IDENTIFIER_TYPES = {"email", "phone", "isikukood", "credit_card"}
INDIRECT_IDENTIFIER_TYPES = {"ip_address", "url"}

PII_WEIGHTS = {
    'email': 1.0,
    'phone': 1.5,
    'isikukood': 3.0,
    'credit_card': 3.0,
    'ip_address': 1.0,
    'url': 0.5,
}

CONFIDENCE_THRESHOLD = 0.3

def _luhn_check(card_number: str) -> bool:
    digits = [int(d) for d in card_number if d.isdigit()]
    if len(digits) < 13 or len(digits) > 19:
        return False
    checksum = 0
    reverse_digits = digits[::-1]
    for i, d in enumerate(reverse_digits):
        if i % 2 == 1:
            d = d * 2
            if d > 9:
                d -= 9
        checksum += d
    return checksum % 10 == 0


def _isikukood_check(code: str) -> bool:
    if len(code) != 11 or not code.isdigit():
        return False
    if code[0] not in '123456':
        return False
    month = int(code[3:5])
    day = int(code[5:7])
    if month < 1 or month > 12 or day < 1 or day > 31:
        return False
    digits = [int(d) for d in code[:10]]
    control = int(code[10])
    weights1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 1]
    rem = sum(d * w for d, w in zip(digits, weights1)) % 11
    if rem < 10:
        return rem == control
    weights2 = [3, 4, 5, 6, 7, 8, 9, 1, 2, 3]
    rem = sum(d * w for d, w in zip(digits, weights2)) % 11
    if rem < 10:
        return rem == control
    return control == 0

def _column_confidence(series: pd.Series, pii_type: str, match_count: int, column_name: str) -> float:
    total = len(series.dropna())
    if total == 0:
        return 0.0

    match_ratio = match_count / total if total > 0 else 0.0
    match_score = min(match_ratio, 1.0) * 0.4

    nunique = series.nunique()
    cardinality_ratio = nunique / total if total > 0 else 0.0
    cardinality_score = cardinality_ratio * 0.3

    canonical = _PATTERN_CANONICAL.get(pii_type, pii_type)
    name_keywords = {
        'email': ['email', 'mail', 'e_mail', 'epost', 'e_post'],
        'phone': ['phone', 'tel', 'mobile', 'cell', 'fax', 'telefon', 'mobiil'],
        'isikukood': ['isikukood', 'isiku_kood', 'ik', 'personal_code', 'personal_id', 'id_kood'],
        'credit_card': ['card', 'credit', 'cc', 'payment', 'kaart', 'krediit'],
        'ip_address': ['ip', 'address', 'host', 'aadress'],
        'url': ['url', 'link', 'href', 'website', 'veebileht'],
    }
    col_lower = column_name.lower()
    keywords = name_keywords.get(canonical, [])
    name_match = 0.3 if any(kw in col_lower for kw in keywords) else 0.0

    return match_score + cardinality_score + name_match

def detect_phone_numbers(series: pd.Series) -> int:
    count = 0
    for value in series.dropna():
        value_str = str(value)
        try:
            parsed = phonenumbers.parse(value_str, None)
            if phonenumbers.is_valid_number(parsed):
                count += 1
        except Exception:
            pass
    return count

def detect_pii_in_column(series: pd.Series, column_name: str) -> Dict[str, int]:
    detected = {}
    series_str = series.astype(str)

    for pii_type, pattern in PII_PATTERNS.items():
        matches = series_str.str.contains(pattern, na=False, regex=True)
        count = matches.sum()
        if count > 0:
            canonical = _PATTERN_CANONICAL.get(pii_type, pii_type)
            if canonical == 'credit_card':
                valid_count = sum(1 for val in series_str[matches] if _luhn_check(val))
                count = valid_count
            elif canonical == 'isikukood':
                valid_count = sum(
                    1 for val in series_str[matches]
                    if _isikukood_check(re.search(pattern, val).group(0))
                )
                count = valid_count
            if count > 0:
                detected[canonical] = detected.get(canonical, 0) + count

    if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
        phone_count = detect_phone_numbers(series)
        if phone_count > 0:
            detected['phone'] = phone_count

    return detected

def analyze_dataset_for_pii(df: pd.DataFrame) -> Tuple[Dict[str, List[str]], Dict[str, int], float, str]:
    sensitive_columns = {}
    pii_counts = Counter()

    column_confidences = {}
    for column in df.columns:
        column_pii = detect_pii_in_column(df[column], column)
        if column_pii:
            col_conf = {}
            accepted_types = []
            for pii_type, count in column_pii.items():
                conf = _column_confidence(df[column], pii_type, count, column)
                col_conf[pii_type] = conf
                if conf >= CONFIDENCE_THRESHOLD:
                    accepted_types.append(pii_type)
                    pii_counts[pii_type] += count
            if accepted_types:
                sensitive_columns[column] = accepted_types
            column_confidences[column] = col_conf

    total_rows = len(df)
    risk_score = 0.0

    for pii_type, count in pii_counts.items():
        weight = PII_WEIGHTS.get(pii_type, 1.0)
        ratio = count / total_rows if total_rows > 0 else 0
        risk_score += weight * ratio * 100

    if risk_score >= 50:
        risk_level = "high"
    elif risk_score >= 20:
        risk_level = "medium"
    else:
        risk_level = "low"

    pii_types_dict = dict(pii_counts)
    sensitive_columns_dict = {col: types for col, types in sensitive_columns.items()}

    return sensitive_columns_dict, pii_types_dict, risk_score, risk_level

def assess_privacy_risk(
    sensitive_columns: Dict[str, List[str]],
    pii_counts: Dict[str, int],
    risk_score: float
) -> Dict[str, object]:
    detected_types = set(pii_counts.keys())
    direct_identifiers = sorted(detected_types.intersection(DIRECT_IDENTIFIER_TYPES))
    indirect_identifiers = sorted(detected_types.intersection(INDIRECT_IDENTIFIER_TYPES))

    if direct_identifiers:
        privacy_status = "blocked"
        summary = (
            "Heuristic privacy screening detected direct identifiers: "
            + ", ".join(direct_identifiers)
            + ". Publish a synthetic or further de-identified dataset instead."
        )
    elif risk_score > 0 or indirect_identifiers:
        privacy_status = "review_required"
        signals = indirect_identifiers or sorted(detected_types)
        signal_text = ", ".join(signals) if signals else "nonzero risk score"
        summary = (
            "Heuristic privacy screening detected indirect identifier signals or nonzero risk "
            f"({signal_text}). Seller approval is required before access."
        )
    else:
        privacy_status = "clear"
        summary = "Heuristic privacy screening did not detect direct identifiers or indirect identifier signals."

    return {
        "status": privacy_status,
        "summary": summary,
        "direct_identifiers": direct_identifiers,
        "indirect_identifiers": indirect_identifiers,
        "sensitive_column_count": len(sensitive_columns),
    }
