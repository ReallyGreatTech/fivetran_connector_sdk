# Bright Data Marketplace Dataset Connector Configuration Guide

## Overview

This connector filters Bright Data Marketplace datasets using the Filter Dataset API
and syncs the filtered results to your Fivetran destination. The connector
dynamically creates tables with flattened dictionary structures,
allowing Fivetran to infer column types automatically.

The Filter Dataset API creates a snapshot by filtering a dataset based on provided
criteria, waits for the snapshot to be ready, and then retrieves the filtered records.

**Important**: All configuration values must be provided as **strings** per Fivetran SDK requirements. This includes numeric values like `records_limit`, which should be provided as `"1000"` instead of `1000`.

See the [Bright Data Filter Dataset API Documentation](https://docs.brightdata.com/api-reference/marketplace-dataset-api/filter-dataset) for more details.

## Configuration Parameters

### Required Parameters

- **`api_token`** (string, required): Your Bright Data API token.
  - Get your API token from: https://brightdata.com/cp/setting/users
  - Format: Bearer token (the connector will automatically add the "Bearer " prefix)

- **`dataset_id`** (string, required): The ID of the dataset to filter.
  - Example: `"gd_l1viktl72bvl7bjuj0"`
  - Find dataset IDs in your Bright Data account dashboard

- **`filter_name`** (string, required): The name of the field to filter on.
  - Example: `"name"`, `"rating"`, `"reviews_count"`, `"category"`
  - Must match a field name in the dataset

- **`filter_operator`** (string, required): The operator to use for filtering.
  - See [Supported Operators](#supported-operators) section below
  - Example: `"="`, `">"`, `">="`, `"includes"`

- **`filter_value`** (string, optional): The value to filter by.
  - Required for all operators except `is_null` and `is_not_null`
  - Example: `"John"`, `"4.5"`, `"200"`
  - For `in` and `not_in` operators, provide comma-separated values

### Optional Parameters

- **`records_limit`** (string, optional): Maximum number of records to include in the snapshot.
  - Must be a positive integer provided as a string
  - Example: `"1000"`, `"5000"`
  - If not provided, all matching records will be included
  - **Note**: All configuration values must be strings per Fivetran SDK requirements

## Supported Operators

The connector supports all operators defined in the [Bright Data Filter Dataset API](https://docs.brightdata.com/api-reference/marketplace-dataset-api/filter-dataset):

### Comparison Operators

- `=`: Equal to
- `!=`: Not equal to
- `<`: Less than
- `<=`: Less than or equal
- `>`: Greater than
- `>=`: Greater than or equal

### Membership Operators

- `in`: Value is in the provided list (use comma-separated values in `filter_value`)
- `not_in`: Value is not in the provided list (use comma-separated values in `filter_value`)

### String Operators

- `includes`: Field value contains the filter value (case-sensitive)
- `not_includes`: Field value does not contain the filter value (case-sensitive)

### Array Operators

- `array_includes`: Filter value is in field value (exact match for arrays)
- `not_array_includes`: Filter value is not in field value (exact match for arrays)

### Null Operators

- `is_null`: Field value is NULL (`filter_value` not required)
- `is_not_null`: Field value is not NULL (`filter_value` not required)

## Example Configurations

### Basic Filter

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_l1viktl72bvl7bjuj0",
  "filter_name": "name",
  "filter_operator": "=",
  "filter_value": "John"
}
```

### Filter with Records Limit

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_l1viktl72bvl7bjuj0",
  "records_limit": "1000",
  "filter_name": "status",
  "filter_operator": "=",
  "filter_value": "active"
}
```

### Numeric Comparison Filter

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_l1viktl72bvl7bjuj0",
  "filter_name": "rating",
  "filter_operator": ">=",
  "filter_value": "4.0"
}
```

### Null Check Filter

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_l1viktl72bvl7bjuj0",
  "filter_name": "description",
  "filter_operator": "is_not_null"
}
```

**Note**: `filter_value` is not required for `is_null` and `is_not_null` operators and will be ignored if provided.

### Contains Filter

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_l1viktl72bvl7bjuj0",
  "filter_name": "title",
  "filter_operator": "includes",
  "filter_value": "example"
}
```

### In Operator Filter (Multiple Values)

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_l1viktl72bvl7bjuj0",
  "filter_name": "category",
  "filter_operator": "in",
  "filter_value": "electronics,books,clothing"
}
```

### Less Than Filter

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_l1viktl72bvl7bjuj0",
  "filter_name": "price",
  "filter_operator": "<",
  "filter_value": "100.00"
}
```

### Not Equal Filter

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_l1viktl72bvl7bjuj0",
  "filter_name": "status",
  "filter_operator": "!=",
  "filter_value": "deleted"
}
```

## How It Works

1. **Configuration Validation**: The connector validates that all required parameters are provided
2. **Filter Object Construction**: The connector builds a filter object from `filter_name`, `filter_operator`, and `filter_value`
3. **Snapshot Creation**: A snapshot is created by sending a POST request to `/datasets/filter` endpoint
4. **Snapshot Polling**: The connector polls for snapshot completion (status changes to "ready")
5. **Record Retrieval**: Once ready, filtered records are retrieved from the snapshot
6. **Data Processing**: Records are flattened and processed with metadata (`dataset_id`, `record_index`)
7. **Schema Discovery**: Fields are dynamically discovered and documented in `fields.yaml`
8. **Data Upsertion**: Processed records are upserted to the `dataset_results` table
9. **State Checkpointing**: Progress is checkpointed to enable resume on interruption

## Notes

- **Snapshot Creation**: The connector creates a snapshot by filtering the dataset, which may take up to 5 minutes to complete
- **Automatic Polling**: The connector automatically polls for snapshot completion (up to 5 minutes with 5-second intervals)
- **Dynamic Schema Discovery**: The connector uses dynamic schema discovery - only primary keys (`dataset_id`, `record_index`) are defined in the schema
- **Field Documentation**: All discovered fields are automatically documented in `fields.yaml`
- **Filter Object**: The connector builds a single-field filter object internally from the provided parameters
- **Null Operators**: For `is_null` and `is_not_null` operators, `filter_value` is not required and will be ignored if provided
- **Records Limit**: The `records_limit` parameter limits the number of records in the snapshot, not the final results
- **Data Flattening**: Nested dictionaries and lists in dataset records are automatically flattened with underscore separators

## API Response Structure

The Filter Dataset API returns:

### Initial Response (Snapshot Creation)
```json
{
  "snapshot_id": "<string>"
}
```

The connector then polls the snapshot metadata endpoint until the status is "ready", then retrieves the filtered records.

## Error Handling

The connector handles various API error responses:

- **400 Bad Request**: Validation errors (e.g., invalid filter structure, invalid `records_limit`)
- **402 Payment Required**: Insufficient account balance
- **422 Unprocessable Entity**: Filter did not match any records
- **429 Too Many Requests**: Maximum limit of 100 parallel jobs per dataset exceeded

The connector includes automatic retry logic with exponential backoff for transient errors (408, 429, 500, 502, 503, 504).

## Troubleshooting

### Configuration Errors

- **Missing filter_value**: Ensure `filter_value` is provided for all operators except `is_null` and `is_not_null`
- **Invalid operator**: Verify the operator is one of the supported operators listed above
- **Invalid records_limit**: Ensure `records_limit` is a positive integer provided as a string (e.g., `"1000"`)

### API Errors

- **Invalid API token**: Verify your API token is correct and has access to the specified dataset
- **Dataset not found**: Ensure the `dataset_id` exists in your Bright Data account
- **Insufficient funds**: Add funds to your Bright Data account if you receive a 402 error
- **No matching records**: The filter may not match any records in the dataset (422 error)
- **Too many parallel jobs**: Reduce the number of concurrent filter operations (429 error)

### Snapshot Errors

- **Snapshot timeout**: If snapshots consistently timeout, consider reducing `records_limit` or checking dataset size
- **Snapshot failed**: Check the snapshot metadata for error details if a snapshot fails
- **Polling timeout**: The connector will timeout after 5 minutes of polling (configurable via constants)

## References

- [Bright Data Filter Dataset API Documentation](https://docs.brightdata.com/api-reference/marketplace-dataset-api/filter-dataset)
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connectors/connector-sdk)
