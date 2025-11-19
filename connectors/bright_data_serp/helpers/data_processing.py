"""Utility helpers for transforming Bright Data SERP responses."""

from typing import Any, Dict, Iterable, Set


def flatten_dict(
    data: Dict[str, Any], parent_key: str = "", sep: str = "_"
) -> Dict[str, Any]:
    """
    Flatten a nested dictionary into a single depth dictionary.
    Nested keys are concatenated using the provided separator.
    """
    items: Dict[str, Any] = {}
    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.update(flatten_dict(value, new_key, sep=sep))
        else:
            items[new_key] = value
    return items


def collect_all_fields(results: Iterable[Dict[str, Any]]) -> Set[str]:
    """Collect the union of keys across all result dictionaries."""
    fields: Set[str] = set()
    for result in results:
        fields.update(result.keys())
    return fields


def process_search_result(result: Any, query: str, result_index: int) -> Dict[str, Any]:
    """
    Transform a raw search result into a flattened dictionary suitable for upsert.

    Primary key fields (query, result_index) are always preserved and never overwritten
    by values from the flattened API response, even if the response contains fields
    with the same names.
    """
    base_fields: Dict[str, Any] = {
        "query": query,
        "result_index": result_index,
        "position": result_index + 1,
    }

    if not isinstance(result, dict):
        base_fields["raw_response"] = str(result)
        return base_fields

    flattened = flatten_dict(result)

    # Remove any conflicting fields from flattened data that match primary key names
    # This ensures API response fields like "query" or "result_index" (if present)
    # don't overwrite our correct primary key values
    # Also check for nested variations (e.g., input.result_index, data.result_index)
    primary_key_fields = {"query", "result_index"}
    for pk_field in primary_key_fields:
        # Remove exact match
        flattened.pop(pk_field, None)
        # Remove any nested variations (e.g., input_result_index, data_result_index)
        keys_to_remove = [
            k
            for k in flattened.keys()
            if k.endswith(f"_{pk_field}") or k.startswith(f"{pk_field}_")
        ]
        for key in keys_to_remove:
            flattened.pop(key, None)

    # Add base_fields after removing conflicts to ensure correct primary keys
    # base_fields is added last to ensure our values (with correct types) always take precedence
    final_result = {**flattened, **base_fields}

    # Explicitly ensure result_index is always an integer
    # This is a safety check - result_index should already be an int from base_fields
    final_result["result_index"] = int(result_index)
    final_result["position"] = int(result_index + 1)

    return final_result
