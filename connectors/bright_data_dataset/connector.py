"""
Bright Data Marketplace Dataset connector built with the Fivetran Connector SDK.

This connector demonstrates how to filter Bright Data datasets using the Marketplace Dataset API
and upsert the filtered results into the Fivetran destination. The connector
dynamically creates tables with flattened dictionary structures,
allowing Fivetran to infer column types automatically.

See the Technical Reference documentation:
https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

See the Best Practices documentation:
https://fivetran.com/docs/connectors/connector-sdk/best-practices
"""

from typing import Any, Dict, List, Optional

# Bright Data SDK for snapshot polling and downloading
from brightdata import bdclient

# Helper functions for data processing, validation, and schema management
from helpers import (
    collect_all_fields,
    filter_dataset,
    process_dataset_record,
    update_fields_yaml,
    validate_configuration,
)

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Table name constant
DATASET_TABLE = "dataset_results"


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
            "table": DATASET_TABLE,
            "primary_key": [
                "dataset_id",
                "record_index",
            ],
        }
    ]


def update(
    configuration: dict, state: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Execute a sync cycle and return the updated connector state.

    Dataset filter supplied in the configuration is applied via Bright Data's
    Marketplace Dataset API, filtered records are flattened, and upserted to the destination.
    Discovered fields are written to `fields.yaml`, and the connector checkpoints
    progress before returning.

    Args:
        configuration: A dictionary containing connection details (api_token, dataset_id, filter_name, filter_operator, filter_value, etc.)
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

    # Initialize Bright Data client with API token for snapshot polling
    api_token = configuration.get("api_token")
    client = bdclient(api_token=api_token)

    new_state = dict(state) if state else {}

    try:
        # Extract configuration parameters
        dataset_id = configuration.get("dataset_id")
        filter_name = configuration.get("filter_name")
        filter_operator = configuration.get("filter_operator")
        filter_value = configuration.get("filter_value")
        records_limit_str = configuration.get("records_limit")

        # Convert records_limit from string to integer if provided
        # All Fivetran SDK configuration values must be strings, so we parse here
        records_limit = None
        if records_limit_str is not None:
            try:
                records_limit = int(records_limit_str)
                if records_limit <= 0:
                    raise ValueError("records_limit must be a positive integer")
            except (ValueError, TypeError) as e:
                raise ValueError(f"records_limit must be a valid positive integer: {str(e)}") from e

        # Build filter object from individual parameters
        # The filter object follows the Bright Data API specification:
        # https://docs.brightdata.com/api-reference/marketplace-dataset-api/filter-dataset
        filter_obj = {
            "name": filter_name,
            "operator": filter_operator,
        }
        # Only include value if operator requires it
        # Operators like 'is_null' and 'is_not_null' don't require a value
        if filter_operator.lower() not in ("is_null", "is_not_null"):
            filter_obj["value"] = filter_value

        # Sync dataset records
        new_state = _sync_dataset_records(
            client=client,
            configuration=configuration,
            dataset_id=dataset_id,
            filter_obj=filter_obj,
            records_limit=records_limit,
            state=new_state,
        )

        # Checkpoint state after processing
        # Save the progress by checkpointing the state. This is important for ensuring that
        # the sync process can resume from the correct position in case of next sync or interruptions.
        # Learn more about checkpointing:
        # https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation
        op.checkpoint(state=new_state)

        return new_state

    except Exception as exc:  # pragma: no cover - bubbled to SDK
        raise RuntimeError(f"Failed to sync data from Bright Data: {str(exc)}") from exc


def _sync_dataset_records(
    client: bdclient,
    configuration: Dict[str, Any],
    dataset_id: str,
    filter_obj: Dict[str, Any],
    records_limit: Optional[int],
    state: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Fetch filtered dataset records and upsert them to Fivetran.

    This function creates a snapshot by filtering the dataset via Bright Data's REST API,
    uses the Bright Data SDK to poll for snapshot completion, retrieves the filtered records,
    and upserts them to the destination. The SDK handles polling automatically.

    Args:
        client: Initialized Bright Data client instance for snapshot polling
        configuration: Configuration dictionary containing API token and other settings
        dataset_id: ID of the dataset to filter
        filter_obj: Filter object containing filter criteria (name, operator, value)
        records_limit: Maximum number of records to include in the snapshot
        state: Current connector state

    Returns:
        Updated state dictionary with sync information

    Raises:
        RuntimeError: If snapshot creation or retrieval fails
    """
    api_token = configuration.get("api_token")

    # Filter dataset and get records
    # This creates a snapshot via REST API, then uses SDK to poll and download
    # See: https://docs.brightdata.com/api-reference/marketplace-dataset-api/filter-dataset
    try:
        records = filter_dataset(
            client=client,
            api_token=api_token,
            dataset_id=dataset_id,
            filter_obj=filter_obj,
            records_limit=records_limit,
        )
    except (RuntimeError, ValueError) as exc:
        # Log error and re-raise for proper error handling at the update level
        log.info(f"Error filtering dataset: {str(exc)}")
        raise
    except Exception as exc:
        # Catch any other exceptions from SDK or API calls
        log.info(f"Unexpected error filtering dataset: {type(exc).__name__}: {str(exc)}")
        raise RuntimeError(
            f"Failed to filter dataset: {str(exc)}"
        ) from exc

    # Normalize results to always be a list
    if not isinstance(records, list):
        records = [records]

    # Process and flatten records
    processed_results: List[Dict[str, Any]] = []
    for index, record in enumerate(records):
        # Each record is flattened and metadata is added (dataset_id, record_index)
        processed_result = process_dataset_record(record, index, dataset_id)
        processed_results.append(processed_result)

    # Upsert processed results to Fivetran
    if processed_results:
        log.info(f"Upserting {len(processed_results)} dataset records to Fivetran")

        # Collect all fields and update schema documentation
        all_fields = collect_all_fields(processed_results)
        update_fields_yaml(all_fields, DATASET_TABLE)

        # Upsert each record as a separate row
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted,
        #   where each key is a column name and the value is a list containing the row value.
        for result in processed_results:
            row: Dict[str, List[Any]] = {
                field: [result.get(field)] for field in all_fields
            }
            op.upsert(DATASET_TABLE, row)

        # Update state with sync information
        state.update(
            {
                "last_dataset_id": dataset_id,
                "last_record_count": len(processed_results),
                "last_filter": filter_obj,
            }
        )

    return state


# Initialize the connector
connector = Connector(update=update, schema=schema)
