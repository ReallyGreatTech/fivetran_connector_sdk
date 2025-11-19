"""
Bright Data SERP connector built with the Fivetran Connector SDK.

This connector demonstrates how to fetch data from Bright Data's SERP REST API
and upsert the flattened results into the Fivetran destination. The connector
dynamically creates tables with flattened dictionary structures,
allowing Fivetran to infer column types automatically.

See the Technical Reference documentation:
https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

See the Best Practices documentation:
https://fivetran.com/docs/connectors/connector-sdk/best-practices
"""

import json
from typing import Any, Dict, List, Optional

# Helper functions for data processing, validation, and schema management
from helpers import (
    collect_all_fields,
    perform_search,
    process_search_result,
    update_fields_yaml,
    validate_configuration,
)

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Table name constant
SERP_TABLE = "search_results"


def schema(_config: dict) -> List[Dict[str, Any]]:
    """
    Declare the destination tables produced by the connector.

    Only the primary keys are defined here; Fivetran infers the remaining column
    metadata from ingested records.

    Args:
        _config: A dictionary that holds the configuration settings for the connector
                 (required by SDK interface but not used for dynamic schema)

    Returns:
        List of table schema definitions with primary keys only.
        Column types are inferred by Fivetran from the data.

    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    """
    return [
        {
            "table": SERP_TABLE,
            "primary_key": [
                "query",
                "result_index",
            ],
        }
    ]


def update(
    configuration: dict, state: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Execute a sync cycle and return the updated connector state.

    Search queries supplied in the configuration are normalized, executed via Bright Data's
    SERP REST API, flattened, and upserted to the destination. Discovered fields
    are written to `fields.yaml`, and the connector checkpoints progress before returning.

    Args:
        configuration: A dictionary containing connection details (api_token, search_query, etc.)
        state: A dictionary containing state information from previous runs.
               The state dictionary is empty for the first sync or for any full re-sync.

    Returns:
        Updated state dictionary with sync information

    Raises:
        ValueError: If configuration validation fails
        RuntimeError: If data sync fails

    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    """
    # Validate the configuration to ensure it contains all required values
    validate_configuration(configuration=configuration)

    api_token = configuration.get("api_token")
    new_state = dict(state) if state else {}

    try:
        search_query_input = configuration.get("search_query", "")
        queries = _parse_search_queries(search_query_input)

        if queries:
            new_state = _sync_search_queries(
                configuration=configuration,
                queries=queries,
                api_token=api_token,
                state=new_state,
            )

        # Checkpoint state after processing all queries
        # Save the progress by checkpointing the state. This is important for ensuring that
        # the sync process can resume from the correct position in case of next sync or interruptions.
        # Learn more about checkpointing:
        # https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation
        op.checkpoint(state=new_state)

        return new_state

    except Exception as exc:  # pragma: no cover - bubbled to SDK
        raise RuntimeError(f"Failed to sync data from Bright Data: {str(exc)}") from exc


def _sync_search_queries(
    configuration: Dict[str, Any],
    queries: List[str],
    api_token: str,
    state: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Fetch search results for the requested queries and upsert them to Fivetran.

    This function processes queries in batch using the Bright Data SERP REST API,
    which handles multiple queries efficiently and maintains query-to-result order.

    Args:
        configuration: Configuration dictionary containing search parameters
        queries: List of search queries to execute
        api_token: Bright Data API token
        state: Current connector state

    Returns:
        Updated state dictionary with sync information
    """
    if not queries:
        raise ValueError("search_query cannot be empty")

    # Extract optional configuration parameters
    search_engine = configuration.get("search_engine")
    country = configuration.get("country")
    search_zone = configuration.get("search_zone")
    format_param = configuration.get("format")

    # Fetch search results for all queries in batch
    # The Bright Data SERP API can handle lists of queries efficiently
    query_payload = queries if len(queries) > 1 else queries[0]
    try:
        search_results = perform_search(
            api_token=api_token,
            query=query_payload,
            search_engine=search_engine,
            country=country,
            zone=search_zone,
            format=format_param,
        )
    except (RuntimeError, ValueError) as exc:
        # Log error and re-raise for proper error handling at the update level
        log.info(f"Error executing search queries: {str(exc)}")
        raise

    # Process and flatten results
    processed_results: List[Dict[str, Any]] = []

    # Match results to queries based on structure
    # The API returns results in the same order as input queries
    if isinstance(search_results, list) and len(queries) > 1:
        # Multiple queries: each query has corresponding results by index
        for query_idx, query in enumerate(queries):
            if query_idx < len(search_results):
                query_results = search_results[query_idx]
                if isinstance(query_results, list):
                    # Multiple results for this query
                    for result_idx, result in enumerate(query_results):
                        processed_results.append(
                            process_search_result(result, query, result_idx)
                        )
                elif isinstance(query_results, dict):
                    # Single result for this query
                    processed_results.append(
                        process_search_result(query_results, query, 0)
                    )
    elif isinstance(search_results, list):
        # Single query with multiple results
        for idx, result in enumerate(search_results):
            processed_results.append(process_search_result(result, queries[0], idx))
    elif isinstance(search_results, dict):
        # Single query with single result
        processed_results.append(process_search_result(search_results, queries[0], 0))

    # Upsert processed results to Fivetran
    if processed_results:
        log.info(f"Upserting {len(processed_results)} search results to Fivetran")

        # Collect all fields and update schema documentation
        all_fields = collect_all_fields(processed_results)
        update_fields_yaml(all_fields, SERP_TABLE)

        # Upsert each result as a separate row
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted,
        #   where each key is a column name and the value is a list containing the row value.
        # Note: Primary key fields (query, result_index) must always be present with correct types
        primary_keys = {"query": str, "result_index": int}
        for result in processed_results:
            # Ensure primary keys are always present with correct types
            for pk, pk_type in primary_keys.items():
                if pk not in result:
                    log.info(f"Warning: Primary key '{pk}' missing from result, adding default value")
                    result[pk] = pk_type() if pk_type == str else 0
                else:
                    # Ensure the type is correct - convert if necessary
                    current_value = result[pk]
                    if not isinstance(current_value, pk_type):
                        try:
                            if pk_type == str:
                                result[pk] = str(current_value)
                            elif pk_type == int:
                                # Try to convert to int, handling JSON strings like "[0]"
                                if isinstance(current_value, str):
                                    # Remove brackets and quotes if it's a JSON string
                                    cleaned = current_value.strip().strip('[]"\'')
                                    result[pk] = int(cleaned) if cleaned.isdigit() else 0
                                else:
                                    result[pk] = int(current_value)
                        except (ValueError, TypeError):
                            log.info(f"Warning: Could not convert primary key '{pk}' to {pk_type.__name__}, using default")
                            result[pk] = pk_type() if pk_type == str else 0

            # Build row data, ensuring primary keys have correct types
            row: Dict[str, List[Any]] = {}
            for field in all_fields:
                value = result.get(field)
                # Explicitly ensure result_index is an integer before upsert
                if field == "result_index":
                    if isinstance(value, str):
                        # Handle string values like "[0]" or "0"
                        cleaned = value.strip().strip('[]"\'')
                        value = int(cleaned) if cleaned.isdigit() else 0
                    elif value is not None:
                        value = int(value)
                    else:
                        value = 0
                row[field] = [value]

            op.upsert(SERP_TABLE, row)

        # Update state with sync information
        state.update(
            {
                "last_search_queries": queries,
                "last_search_count": len(processed_results),
            }
        )

    return state


def _parse_search_queries(search_query_input: Any) -> List[str]:
    """
    Normalize the search_query configuration value into a list of queries.

    Supports multiple input formats:
    - JSON string containing a list: '["query1", "query2"]'
    - JSON string containing a single query: '"query"'
    - Comma-separated string: "query1,query2,query3"
    - Newline-separated string: "query1\nquery2\nquery3"
    - Single query string: "query"
    - Python list: ["query1", "query2"]

    Args:
        search_query_input: The search_query configuration value (various formats supported)

    Returns:
        List of normalized query strings
    """
    if not search_query_input:
        return []

    if isinstance(search_query_input, list):
        return [
            item.strip()
            for item in search_query_input
            if isinstance(item, str) and item.strip()
        ]

    if isinstance(search_query_input, str):
        # Try parsing as JSON first
        try:
            parsed = json.loads(search_query_input)
            if isinstance(parsed, list):
                return [
                    item.strip()
                    for item in parsed
                    if isinstance(item, str) and item.strip()
                ]
            if isinstance(parsed, str) and parsed.strip():
                return [parsed.strip()]
        except (json.JSONDecodeError, TypeError):
            pass

        # Try comma-separated format
        if "," in search_query_input:
            return [
                item.strip() for item in search_query_input.split(",") if item.strip()
            ]

        # Try newline-separated format
        if "\n" in search_query_input:
            return [
                item.strip() for item in search_query_input.split("\n") if item.strip()
            ]

        # Single query
        return [search_query_input.strip()] if search_query_input.strip() else []

    return []


# Initialize the connector
connector = Connector(update=update, schema=schema)
