"""
Bright Data Web Scraper connector built with the Fivetran Connector SDK.

This connector demonstrates how to fetch data from Bright Data's Web Scraper API
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

# Bright Data SDK for web scraping functionality
from brightdata import bdclient

# Helper functions for data processing, validation, and schema management
from helpers import (
    collect_all_fields,
    perform_scrape,
    process_scrape_result,
    update_fields_yaml,
    validate_configuration,
)

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Table name constant
SCRAPE_TABLE = "scrape_results"


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
            "table": SCRAPE_TABLE,
            "primary_key": [
                "url",
                "result_index",
            ],
        }
    ]


def update(
    configuration: dict, state: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Execute a sync cycle and return the updated connector state.

    URLs supplied in the configuration are normalized, scraped via Bright Data's
    Web Scraper API, flattened, and upserted to the destination. Discovered fields
    are written to `fields.yaml`, and the connector checkpoints progress before returning.

    Args:
        configuration: A dictionary containing connection details (api_token, scrape_url, etc.)
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

    # Initialize Bright Data client with API token
    api_token = configuration.get("api_token")
    client = bdclient(api_token=api_token)

    new_state = dict(state) if state else {}

    try:
        scrape_url_input = configuration.get("scrape_url", "")
        urls = _parse_scrape_urls(scrape_url_input)

        if urls:
            new_state = _sync_scrape_urls(
                client=client,
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


def _sync_scrape_urls(
    client: bdclient,
    configuration: Dict[str, Any],
    urls: List[str],
    state: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Fetch scrape results for the requested URLs and upsert them to Fivetran.

    This function processes URLs in batch using the Bright Data SDK, which handles
    parallel processing, async snapshot creation, and polling automatically.
    The SDK maintains URL-to-result order for proper mapping.

    Args:
        client: Initialized Bright Data client instance
        configuration: Configuration dictionary containing scrape parameters
        urls: List of URLs to scrape (processed in batch by SDK)
        state: Current connector state

    Returns:
        Updated state dictionary with sync information
    """
    if not urls:
        raise ValueError("scrape_url cannot be empty")

    # Extract optional configuration parameters
    country = configuration.get("country")
    data_format = configuration.get("data_format")
    format_param = configuration.get("format")
    method = configuration.get("method")

    # Parse async_request configuration (defaults to True)
    async_request = True
    async_request_str = str(configuration.get("async_request", "")).lower()
    if async_request_str in ("false", "0", "no"):
        async_request = False

    # Fetch scrape results for all URLs in batch
    # The Bright Data SDK can handle lists of URLs efficiently, processing them in parallel
    # and managing async snapshot creation and polling automatically
    url_payload = urls if len(urls) > 1 else urls[0]
    try:
        scrape_results = perform_scrape(
            client=client,
            url=url_payload,
            country=country,
            data_format=data_format,
            format_param=format_param,
            method=method,
            async_request=async_request,
        )
    except (RuntimeError, ValueError) as exc:
        # Log error and re-raise for proper error handling at the update level
        log.info(f"Error scraping URLs: {str(exc)}")
        raise

    # Normalize results to always be a list
    if not isinstance(scrape_results, list):
        scrape_results = [scrape_results]

    # Process and flatten results
    # The SDK returns results in the same order as input URLs (one result per URL)
    # Each result may contain multiple items if the scraped content has multiple elements
    processed_results: List[Dict[str, Any]] = []

    # Process each result and match it to the corresponding URL by index
    # The SDK maintains URL-to-result order when processing in batch
    for url_idx, url in enumerate(urls):
        # Get the result for this URL (by index)
        if url_idx < len(scrape_results):
            result = scrape_results[url_idx]

            # Handle cases where one URL produces multiple results
            if isinstance(result, list):
                # Multiple results from one URL (e.g., multiple elements extracted)
                for item_idx, item in enumerate(result):
                    processed_results.append(
                        process_scrape_result(item, url, item_idx)
                    )
            else:
                # Single result per URL
                processed_results.append(process_scrape_result(result, url, 0))
        else:
            # Fewer results than URLs (some URLs may have failed silently)
            # Log and skip this URL
            log.info(f"Warning: No result found for URL at index {url_idx}: {url}")

    # Upsert processed results to Fivetran
    if processed_results:
        log.info(f"Upserting {len(processed_results)} scrape results to Fivetran")

        # Collect all fields and update schema documentation
        all_fields = collect_all_fields(processed_results)
        update_fields_yaml(all_fields, SCRAPE_TABLE)

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
            op.upsert(SCRAPE_TABLE, row)

        # Update state with sync information
        state.update(
            {
                "last_scrape_urls": urls,
                "last_scrape_count": len(processed_results),
            }
        )

    return state


def _parse_scrape_urls(scrape_url_input: Any) -> List[str]:
    """
    Normalize the scrape_url configuration value into a list of URLs.

    Supports multiple input formats:
    - JSON string containing a list: '["url1", "url2"]'
    - JSON string containing a single URL: '"url"'
    - Comma-separated string: "url1,url2,url3"
    - Newline-separated string: "url1\nurl2\nurl3"
    - Single URL string: "url"
    - Python list: ["url1", "url2"]

    Args:
        scrape_url_input: The scrape_url configuration value (various formats supported)

    Returns:
        List of normalized URL strings
    """
    if not scrape_url_input:
        return []

    if isinstance(scrape_url_input, list):
        return [
            item.strip() for item in scrape_url_input if isinstance(item, str) and item.strip()
        ]

    if isinstance(scrape_url_input, str):
        # Try parsing as JSON first
        try:
            parsed = json.loads(scrape_url_input)
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
        if "," in scrape_url_input:
            return [item.strip() for item in scrape_url_input.split(",") if item.strip()]

        # Try newline-separated format
        if "\n" in scrape_url_input:
            return [item.strip() for item in scrape_url_input.split("\n") if item.strip()]

        # Single URL
        return [scrape_url_input.strip()] if scrape_url_input.strip() else []

    return []


# Initialize the connector
connector = Connector(update=update, schema=schema)
