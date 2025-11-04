import re
import pandas as pd
import phonenumbers
from typing import List, Dict, Tuple
from collections import Counter

PII_PATTERNS = {
    'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
    'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
    'credit_card': r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b',
    'ip_address': r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
    'url': r'https?://[^\s]+',
}

PII_WEIGHTS = {
    'email': 1.0,
    'phone': 1.5,
    'ssn': 3.0,
    'credit_card': 3.0,
    'ip_address': 1.0,
    'url': 0.5,
}

def detect_phone_numbers(series: pd.Series) -> int:
    count = 0
    for value in series.dropna():
        value_str = str(value)
        try:
            parsed = phonenumbers.parse(value_str, None)
            if phonenumbers.is_valid_number(parsed):
                count += 1
        except:
            pass
    return count

def detect_pii_in_column(series: pd.Series, column_name: str) -> Dict[str, int]:
    detected = {}
    column_lower = column_name.lower()
    series_str = series.astype(str)
    
    if 'email' in column_lower or 'mail' in column_lower:
        matches = series_str.str.contains(PII_PATTERNS['email'], na=False, regex=True)
        detected['email'] = matches.sum()
    
    if 'phone' in column_lower or 'tel' in column_lower:
        detected['phone'] = detect_phone_numbers(series)
    
    if 'ssn' in column_lower or 'social' in column_lower:
        matches = series_str.str.contains(PII_PATTERNS['ssn'], na=False, regex=True)
        detected['ssn'] = matches.sum()
    
    if 'card' in column_lower or 'credit' in column_lower:
        matches = series_str.str.contains(PII_PATTERNS['credit_card'], na=False, regex=True)
        detected['credit_card'] = matches.sum()
    
    if 'ip' in column_lower:
        matches = series_str.str.contains(PII_PATTERNS['ip_address'], na=False, regex=True)
        detected['ip_address'] = matches.sum()
    
    for pii_type, pattern in PII_PATTERNS.items():
        if pii_type not in detected:
            matches = series_str.str.contains(pattern, na=False, regex=True)
            count = matches.sum()
            if count > 0:
                detected[pii_type] = count
    
    return detected

def analyze_dataset_for_pii(df: pd.DataFrame) -> Tuple[Dict[str, List[str]], Dict[str, int], float, str]:
    sensitive_columns = {}
    pii_counts = Counter()
    
    for column in df.columns:
        column_pii = detect_pii_in_column(df[column], column)
        if column_pii:
            sensitive_columns[column] = list(column_pii.keys())
            for pii_type, count in column_pii.items():
                pii_counts[pii_type] += count
    
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

