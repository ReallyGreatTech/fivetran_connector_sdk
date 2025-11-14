"""
Bright Data Fivetran Connector

This connector demonstrates how to fetch data from Bright Data's Web Unlocker API
and upsert it into the Fivetran destination. The connector
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
    perform_web_unlocker,
    process_unlocker_result,
    update_fields_yaml,
    validate_configuration,
)

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Table name constant
UNLOCKER_TABLE = "unlocker_results"


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
            "table": UNLOCKER_TABLE,
            "primary_key": [
                "requested_url",
                "result_index",
            ],
        }
    ]


def update(
    configuration: dict, state: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Execute a sync cycle and return the updated connector state.

    URLs supplied in the configuration are normalized, fetched via Bright Data's
    Web Unlocker API, flattened, and upserted to the destination. Discovered fields
    are written to `fields.yaml`, and the connector checkpoints progress before returning.

    Args:
        configuration: A dictionary containing connection details (api_token, unlocker_url, etc.)
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

    new_state = dict(state) if state else {}

    try:
        unlocker_url_input = configuration.get("unlocker_url", "")
        urls = _parse_unlocker_urls(unlocker_url_input)

        if urls:
            new_state = _sync_unlocker_urls(
                configuration=configuration,
                urls=urls,
                state=new_state,
            )

        # Checkpoint state after processing all URLs
        # Save the progress by checkpointing the state. This is important for ensuring that
        # the sync process can resume from the correct position in case of next sync or interruptions.
        # Learn more about checkpointing:
        # https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation
        op.checkpoint(state=new_state)

        return new_state

    except Exception as exc:  # pragma: no cover - bubbled to SDK
        raise RuntimeError(f"Failed to sync data from Bright Data: {str(exc)}") from exc


def _sync_unlocker_urls(
    configuration: Dict[str, Any],
    urls: List[str],
    state: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Fetch unlocker results for the requested URLs and upsert them to Fivetran.

    This function processes URLs in batch using the Bright Data Web Unlocker API,
    which handles multiple URLs efficiently and maintains URL-to-result order.

    Args:
        configuration: Configuration dictionary containing unlocker parameters
        urls: List of URLs to unlock/fetch
        state: Current connector state

    Returns:
        Updated state dictionary with sync information
    """
    if not urls:
        raise ValueError("unlocker_url cannot be empty")

    # Extract optional configuration parameters
    api_token = configuration.get("api_token")
    country = configuration.get("country")
    data_format = configuration.get("data_format")
    format_param = configuration.get("format_param")
    method = configuration.get("method") or "GET"
    unlocker_zone = configuration.get("zone")

    # Fetch unlocker results for all URLs in batch
    # The Bright Data Web Unlocker API can handle lists of URLs efficiently
    payload = urls if len(urls) > 1 else urls[0]
    try:
        unlocker_results = perform_web_unlocker(
            api_token=api_token,
            url=payload,
            zone=unlocker_zone,
            country=country,
            method=method,
            format_param=format_param,
            data_format=data_format,
        )
    except (RuntimeError, ValueError) as exc:
        # Log error and re-raise for proper error handling at the update level
        log.info(f"Error fetching unlocker results: {str(exc)}")
        raise

    # Normalize results to always be a list
    if not isinstance(unlocker_results, list):
        unlocker_results = [unlocker_results]

    # Process and flatten results
    processed_results: List[Dict[str, Any]] = []
    for index, result in enumerate(unlocker_results):
        # Use requested_url from result if available, otherwise fall back to input URL
        requested_url = result.get("requested_url") or urls[index % len(urls)]
        processed_results.append(process_unlocker_result(result, requested_url, index))

    # Upsert processed results to Fivetran
    if processed_results:
        log.info(f"Upserting {len(processed_results)} unlocker results to Fivetran")

        # Collect all fields and update schema documentation
        all_fields = collect_all_fields(processed_results)
        update_fields_yaml(all_fields, UNLOCKER_TABLE)

        # Upsert each result as a separate row
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted,
        #   where each key is a column name and the value is a list containing the row value.
        for result in processed_results:
            row: Dict[str, List[Any]] = {
                field: [result.get(field)] for field in all_fields
            }
            op.upsert(UNLOCKER_TABLE, row)

        # Update state with sync information
        state.update(
            {
                "last_unlocker_urls": urls,
                "last_unlocker_count": len(processed_results),
            }
        )

    return state


def _parse_unlocker_urls(unlocker_url_input: Any) -> List[str]:
    """
    Normalize the unlocker_url configuration value into a list of URLs.

    Supports multiple input formats:
    - JSON string containing a list: '["url1", "url2"]'
    - JSON string containing a single URL: '"url"'
    - Comma-separated string: "url1,url2,url3"
    - Newline-separated string: "url1\nurl2\nurl3"
    - Single URL string: "url"
    - Python list: ["url1", "url2"]

    Args:
        unlocker_url_input: The unlocker_url configuration value (various formats supported)

    Returns:
        List of normalized URL strings
    """
    if not unlocker_url_input:
        return []

    if isinstance(unlocker_url_input, list):
        return [
            item.strip() for item in unlocker_url_input if isinstance(item, str) and item.strip()
        ]

    if isinstance(unlocker_url_input, str):
        # Try parsing as JSON first
        try:
            parsed = json.loads(unlocker_url_input)
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
        if "," in unlocker_url_input:
            return [
                item.strip() for item in unlocker_url_input.split(",") if item.strip()
            ]

        # Try newline-separated format
        if "\n" in unlocker_url_input:
            return [
                item.strip() for item in unlocker_url_input.split("\n") if item.strip()
            ]

        # Single URL
        return [unlocker_url_input.strip()] if unlocker_url_input.strip() else []

    return []


# Initialize the connector
connector = Connector(update=update, schema=schema)
