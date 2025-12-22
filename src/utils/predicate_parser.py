import re
import pyarrow.dataset as ds
import pyarrow as pa
from typing import List, Dict, Any, Optional, Union, Tuple
from fastapi import HTTPException

SUPPORTED_COMPARISON_OPS = ["=", "!=", ">", "<", ">=", "<="]
SUPPORTED_SET_OPS = ["IN"]
SUPPORTED_NULL_OPS = ["IS NULL", "IS NOT NULL"]
MAX_PREDICATES = 20
MAX_IN_LIST_SIZE = 1000

class PredicateNode:
    def __init__(self, op: str, column: str, value: Any = None, values: List[Any] = None):
        self.op = op
        self.column = column
        self.value = value
        self.values = values

def parse_predicate_string(predicate_str: str) -> PredicateNode:
    predicate_str = predicate_str.strip()
    
    if " IS NOT NULL" in predicate_str.upper():
        column = predicate_str[:predicate_str.upper().index(" IS NOT NULL")].strip()
        return PredicateNode("IS NOT NULL", column)
    
    if " IS NULL" in predicate_str.upper():
        column = predicate_str[:predicate_str.upper().index(" IS NULL")].strip()
        return PredicateNode("IS NULL", column)
    
    for op in ["!=", ">=", "<=", "=", ">", "<"]:
        if f" {op} " in predicate_str:
            parts = predicate_str.split(f" {op} ", 1)
            if len(parts) == 2:
                column = parts[0].strip()
                value_str = parts[1].strip()
                value = parse_value(value_str)
                return PredicateNode(op, column, value=value)
    
    if " IN " in predicate_str.upper():
        match = re.match(r'^(.+?)\s+IN\s+\((.+)\)$', predicate_str, re.IGNORECASE)
        if match:
            column = match.group(1).strip()
            values_str = match.group(2).strip()
            values = parse_in_list(values_str)
            return PredicateNode("IN", column, values=values)
    
    raise HTTPException(status_code=400, detail=f"Unsupported predicate format: {predicate_str}")

def parse_value(value_str: str) -> Any:
    value_str = value_str.strip()
    
    if value_str.startswith("'") and value_str.endswith("'"):
        return value_str[1:-1]
    if value_str.startswith('"') and value_str.endswith('"'):
        return value_str[1:-1]
    
    if value_str.upper() == "TRUE":
        return True
    if value_str.upper() == "FALSE":
        return False
    if value_str.upper() == "NULL":
        return None
    
    try:
        if '.' in value_str:
            return float(value_str)
        return int(value_str)
    except ValueError:
        return value_str

def parse_in_list(values_str: str) -> List[Any]:
    values = []
    current = ""
    in_quotes = False
    quote_char = None
    
    for char in values_str:
        if char in ("'", '"') and (not in_quotes or char == quote_char):
            if not in_quotes:
                in_quotes = True
                quote_char = char
            else:
                in_quotes = False
                quote_char = None
            current += char
        elif char == ',' and not in_quotes:
            if current.strip():
                values.append(parse_value(current.strip()))
            current = ""
        else:
            current += char
    
    if current.strip():
        values.append(parse_value(current.strip()))
    
    return values

def parse_predicate_hints(predicate_hints: Union[List[str], str]) -> List[PredicateNode]:
    if isinstance(predicate_hints, str):
        predicate_hints = [predicate_hints]
    
    if len(predicate_hints) > MAX_PREDICATES:
        raise HTTPException(status_code=400, detail=f"Too many predicates (max {MAX_PREDICATES})")
    
    predicates = []
    for hint in predicate_hints:
        try:
            predicate = parse_predicate_string(hint)
            predicates.append(predicate)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid predicate: {hint}. Error: {str(e)}")
    
    return predicates

def parse_json_predicate_hints(json_predicates: Union[List[Dict], Dict]) -> List[PredicateNode]:
    if isinstance(json_predicates, dict):
        json_predicates = [json_predicates]
    
    if len(json_predicates) > MAX_PREDICATES:
        raise HTTPException(status_code=400, detail=f"Too many predicates (max {MAX_PREDICATES})")
    
    predicates = []
    for pred_dict in json_predicates:
        if not isinstance(pred_dict, dict):
            raise HTTPException(status_code=400, detail=f"Invalid JSON predicate format: {pred_dict}")
        
        column = pred_dict.get("column") or pred_dict.get("col")
        op = pred_dict.get("op") or pred_dict.get("operator")
        value = pred_dict.get("value")
        values = pred_dict.get("values")
        
        if not column:
            raise HTTPException(status_code=400, detail="Missing 'column' in JSON predicate")
        if not op:
            raise HTTPException(status_code=400, detail="Missing 'op' in JSON predicate")
        
        if op.upper() in ["IS NULL", "IS NOT NULL"]:
            predicates.append(PredicateNode(op.upper(), column))
        elif op.upper() == "IN":
            if not values:
                raise HTTPException(status_code=400, detail="IN operator requires 'values' array")
            if len(values) > MAX_IN_LIST_SIZE:
                raise HTTPException(status_code=400, detail=f"IN list too large (max {MAX_IN_LIST_SIZE})")
            predicates.append(PredicateNode("IN", column, values=values))
        else:
            if value is None:
                raise HTTPException(status_code=400, detail=f"Operator {op} requires 'value'")
            predicates.append(PredicateNode(op, column, value=value))
    
    return predicates

def validate_predicates(predicates: List[PredicateNode], schema: pa.Schema) -> None:
    schema_columns = set(schema.names)
    
    for predicate in predicates:
        if predicate.column not in schema_columns:
            raise HTTPException(status_code=400, detail=f"Column '{predicate.column}' not found in table schema")
        
        if predicate.op not in SUPPORTED_COMPARISON_OPS + SUPPORTED_SET_OPS + SUPPORTED_NULL_OPS:
            raise HTTPException(status_code=400, detail=f"Unsupported operator: {predicate.op}")
        
        if predicate.op == "IN" and len(predicate.values) > MAX_IN_LIST_SIZE:
            raise HTTPException(status_code=400, detail=f"IN list too large (max {MAX_IN_LIST_SIZE})")

def predicate_to_pyarrow_expr(predicate: PredicateNode) -> ds.Expression:
    field = ds.field(predicate.column)
    
    if predicate.op == "=":
        return field == predicate.value
    elif predicate.op == "!=":
        return field != predicate.value
    elif predicate.op == ">":
        return field > predicate.value
    elif predicate.op == "<":
        return field < predicate.value
    elif predicate.op == ">=":
        return field >= predicate.value
    elif predicate.op == "<=":
        return field <= predicate.value
    elif predicate.op == "IN":
        return field.isin(predicate.values)
    elif predicate.op == "IS NULL":
        return field.is_null()
    elif predicate.op == "IS NOT NULL":
        return field.is_valid()
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported operator: {predicate.op}")

def predicates_to_pyarrow_filter(predicates: List[PredicateNode], schema: pa.Schema) -> Optional[ds.Expression]:
    if not predicates:
        return None
    
    validate_predicates(predicates, schema)
    
    expressions = [predicate_to_pyarrow_expr(pred) for pred in predicates]
    
    if len(expressions) == 1:
        return expressions[0]
    
    result = expressions[0]
    for expr in expressions[1:]:
        result = result & expr
    
    return result

def parse_query_predicates(body: Dict[str, Any], schema: pa.Schema) -> Tuple[Optional[ds.Expression], List[PredicateNode]]:
    predicates = []
    
    if "jsonPredicateHints" in body:
        predicates = parse_json_predicate_hints(body["jsonPredicateHints"])
    elif "predicateHints" in body:
        predicates = parse_predicate_hints(body["predicateHints"])
    
    if not predicates:
        return None, []
    
    filter_expr = predicates_to_pyarrow_filter(predicates, schema)
    return filter_expr, predicates

