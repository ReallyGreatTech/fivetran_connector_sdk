# Bright Data Web Scraper Connector

## Table of Contents

- [Overview](#overview)
- [How It Works](#how-it-works)
  - [1. Configuration Validation](#1-configuration-validation)
  - [2. URL Normalization](#2-url-normalization)
  - [3. Scrape Job Triggering](#3-scrape-job-triggering)
  - [4. Snapshot Polling](#4-snapshot-polling)
  - [5. Data Processing](#5-data-processing)
  - [6. Schema Discovery](#6-schema-discovery)
  - [7. Data Upsertion](#7-data-upsertion)
  - [8. State Checkpointing](#8-state-checkpointing)
- [API Endpoints Used](#api-endpoints-used)
  - [POST `/datasets/v3/trigger`](#post-datasetsv3trigger)
  - [GET `/datasets/v3/snapshot/{snapshot_id}`](#get-datasetsv3snapshotsnapshot_id)
- [Implementation Details](#implementation-details)
  - [Asynchronous Processing](#asynchronous-processing)
  - [Snapshot Polling](#snapshot-polling-1)
  - [Error Handling](#error-handling)
  - [Schema Discovery](#schema-discovery-1)
  - [Data Flattening](#data-flattening)
  - [Batch Processing](#batch-processing)
  - [Result Indexing](#result-indexing)
- [Configuration Parameters](#configuration-parameters)
- [Troubleshooting](#troubleshooting)
  - [Configuration Errors](#configuration-errors)
  - [Scraping Errors](#scraping-errors)
  - [API Errors](#api-errors)
  - [Response Parsing Errors](#response-parsing-errors)
- [Response Format Handling](#response-format-handling)
  - [JSON Array](#json-array)
  - [JSON Lines (JSONL)](#json-lines-jsonl)
  - [String Response](#string-response)
  - [Status Response](#status-response)
- [Notes](#notes)
- [References](#references)
- [Development](#development)
  - [Prerequisites](#prerequisites)
  - [Local Debugging](#local-debugging)
  - [Deploying to Fivetran](#deploying-to-fivetran)
  - [Best Practices](#best-practices)
  - [Common Debugging Issues](#common-debugging-issues)

## Overview

This connector fetches data from Bright Data's Web Scraper REST API and syncs the scraped results to your Fivetran destination. The connector uses the Bright Data asynchronous scraping endpoint (`/datasets/v3/trigger`) to trigger scrape jobs and polls the snapshot endpoint (`/datasets/v3/snapshot/{snapshot_id}`) until results are ready. The connector dynamically creates tables with flattened dictionary structures, allowing Fivetran to infer column types automatically.

**Important**: All configuration values must be provided as **strings** per Fivetran SDK requirements.

## How It Works

### 1. Configuration Validation
The connector validates that `api_token`, `dataset_id`, and `scrape_url` are provided.

### 2. URL Normalization
The connector parses and normalizes the `scrape_url` input into a list of URLs, supporting multiple input formats:
- JSON array string
- Comma-separated URLs
- Newline-separated URLs
- Single URL

### 3. Scrape Job Triggering
The connector sends a POST request to `/datasets/v3/trigger` with the URLs to scrape:
- Request body: Array of URL objects `[{"url": "https://..."}, ...]`
- Query parameters: `dataset_id`, `format`, `include_errors`
- Dataset-specific parameters: Certain datasets may require additional query parameters (e.g., `discover_by`, `type`)

### 4. Snapshot Polling
The connector receives a `snapshot_id` and polls `/datasets/v3/snapshot/{snapshot_id}` until results are ready:
- Polling continues indefinitely until the snapshot is ready, failed, or an error occurs
- Default polling interval is 30 seconds (configurable via API recommendations)
- The endpoint handles polling internally, so the connector waits for completion
- Supports multiple response formats: JSON arrays, JSON Lines (JSONL), and string responses

### 5. Data Processing
When the snapshot is ready, scraped results are:
- Flattened to handle nested dictionaries and lists
- Processed with underscore separators for nested fields
- Assigned unique `result_index` values (0-based) for primary key uniqueness

### 6. Schema Discovery
Fields are dynamically discovered and documented in `fields.yaml`. Only primary keys (`url`, `result_index`, `_fivetran_synced`) are defined in the schema - all other columns are inferred by Fivetran.

### 7. Data Upsertion
Processed records are upserted to the `scrape_results` table with proper primary key handling.

### 8. State Checkpointing
Progress is checkpointed to enable resume on interruption.

## API Endpoints Used

This connector uses the following Bright Data REST API endpoints:

### POST `/datasets/v3/trigger`
Triggers an asynchronous scrape job.

**Query Parameters:**
- `dataset_id` (required): The ID of the dataset to use for scraping
- `format`: Always set to "json"
- `include_errors`: Always set to "true" to include errors report with the results

**Request Body:**
```json
[
  {"url": "https://www.example.com/1"},
  {"url": "https://www.example.com/2"},
  ...
]
```

**Response:**
```json
{
  "snapshot_id": "s_..."
}
```

**Dataset-Specific Parameters:**
Certain datasets may require additional query parameters. For example, dataset `gd_lyy3tktm25m4avu764` automatically adds:
- `discover_by=profile_url`
- `type=discover_new`

### GET `/datasets/v3/snapshot/{snapshot_id}`
Polls for snapshot completion and retrieves results.

**Query Parameters:**
- `format`: Always set to "json"
- `include_errors`: Always set to "true" to include errors report

**Response Formats:**
- **When ready**: Array of scraped results `[{...}, {...}, ...]`
- **JSON Lines**: Multiple JSON objects separated by newlines
- **While processing**: Status messages or HTTP 202 (Accepted) status

**Status Handling:**
- `200` with array/list: Snapshot is ready, return data
- `200` with dict containing `status: "ready"`: Extract data and return
- `200` with dict containing `status: "failed"`: Raise error with failure details
- `200` with dict containing `status: "running"|"pending"|"processing"`: Continue polling
- `202`: Snapshot is not ready yet, wait 30 seconds and retry
- `404`: Snapshot not found, raise error
- Other errors: Log and retry based on status code

## Implementation Details

### Asynchronous Processing
All scraping is done asynchronously using Bright Data's snapshot-based API. The connector automatically handles job triggering and polling.

### Snapshot Polling
The connector polls the snapshot endpoint indefinitely until the snapshot is ready or failed. There is no hard limit on polling attempts - the connector relies on the API's response status and request timeouts.


### Error Handling
The connector includes comprehensive error handling for:
- **API errors** (400, 402, 404, 422, 429): Specific error messages extracted from response
- **Network failures**: Automatic retries with exponential backoff
- **Invalid responses**: Parsing errors with fallback to alternative formats
- **Failed snapshots**: Detailed error messages logged

### Schema Discovery
The connector uses dynamic schema discovery - only primary keys (`url`, `result_index`, `_fivetran_synced`) are defined in the schema. All other fields are discovered from the data and documented in `fields.yaml`.

### Data Flattening
Nested dictionaries and lists are automatically flattened with underscore separators. For example:
```json
{
  "user": {
    "name": "John",
    "details": {
      "age": 30
    }
  }
}
```
becomes:
```json
{
  "user_name": "John",
  "user_details_age": 30
}
```

### Batch Processing
Multiple URLs are processed in a single batch request, with results returned as an array of objects. Each result in the array is assigned a unique `result_index` (0-based) to ensure proper primary key uniqueness.

### Result Indexing
Each result in the array is assigned a unique `result_index` (0-based) to ensure proper primary key uniqueness. This is critical for handling multiple results from a single URL.

## Configuration Parameters

See [CONFIGURATION_GUIDE.md](./CONFIGURATION_GUIDE.md) for detailed configuration information.

### Required Parameters
- `api_token`: Bright Data API token
- `dataset_id`: Bright Data dataset ID
- `scrape_url`: URL(s) to scrape


## Troubleshooting

### Configuration Errors

- **Missing api_token**: Ensure `api_token` is provided as a string
- **Missing dataset_id**: Ensure `dataset_id` is provided as a string
- **Empty scrape_url**: Ensure `scrape_url` is provided and not empty
- **Invalid dataset_id**: Verify your dataset ID is correct and accessible in your Bright Data account

### Scraping Errors

- **URL not accessible**: Verify the URL is publicly accessible or accessible via Bright Data
- **Snapshot not ready**: If snapshots don't complete, check the Bright Data dashboard for job status
- **Invalid input provided** (400/422 errors): Check your dataset configuration and ensure URLs are valid
- **Rate limiting** (429 errors): If you encounter rate limits, consider reducing the number of URLs or waiting before retrying
- **Snapshot failed**: Check the Bright Data dashboard for detailed error information about why the snapshot failed
- **Snapshot not found** (404 errors): The snapshot ID may be invalid or the snapshot may have expired. Retry the scrape job.

### API Errors

- **Invalid API token**: Verify your API token is correct and has access to the Web Scraper API
- **Missing permissions**: Ensure your Bright Data account has access to the Web Scraper feature and the specific dataset
- **Dataset access**: Verify that your account has access to the dataset specified in `dataset_id`
- **Insufficient balance** (402 errors): Add funds to your Bright Data account

### Response Parsing Errors

- **JSON parse error**: The connector automatically handles JSON Lines format and string responses. If parsing fails, check the Bright Data dashboard for the snapshot status.
- **Unexpected response format**: The connector supports multiple response formats (JSON arrays, JSON Lines, string responses). If you encounter issues, check the dataset documentation.

## Response Format Handling

The connector handles multiple response formats from the Bright Data API:

### JSON Array
```json
[
  {"id": "1", "name": "John"},
  {"id": "2", "name": "Jane"}
]
```

### JSON Lines (JSONL)
```
{"id": "1", "name": "John"}
{"id": "2", "name": "Jane"}
```

### String Response
When the API returns a JSON string instead of a parsed object:
```json
"{\"id\": \"1\", \"name\": \"John\"}"
```
The connector automatically parses it.

### Status Response
When the snapshot is still processing:
```json
{
  "status": "running",
  "snapshot_id": "s_..."
}
```

## Notes

- **Polling Behavior**: The connector polls indefinitely until the snapshot is ready or failed. There is no hard limit on polling attempts.
- **Request Timeouts**: Individual API requests use configurable timeouts (default: 120 seconds).
- **Retry Logic**: Failed requests are retried with exponential backoff for transient errors (500, 502, 503, 504).
- **Error Messages**: All errors are logged with detailed information for debugging.
- **Field Documentation**: Discovered fields are automatically documented in `fields.yaml` for reference.

## References

- [Bright Data Web Scraper REST API Documentation](https://docs.brightdata.com/api-reference/rest-api/scraper/asynchronous-requests)
- [Bright Data Web Scraper Overview](https://docs.brightdata.com/)
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connectors/connector-sdk)
- [Fivetran Connector SDK Best Practices](https://fivetran.com/docs/connectors/connector-sdk/best-practices)



## Development

### Prerequisites

Before debugging or deploying the connector, ensure you have:

1. **Python 3.8+** installed
2. **Fivetran Connector SDK** installed:
   ```bash
   pip install fivetran-connector-sdk
   ```
3. **Project dependencies** installed:
   ```bash
   pip install -r requirements.txt
   ```
4. A valid `configuration.json` file in the connector directory

### Local Debugging

The Fivetran SDK provides a `debug` command that allows you to test your connector locally before deploying to Fivetran.

#### 1. Create Configuration File

Create a `configuration.json` file in the connector directory with your test configuration:

```json
{
  "api_token": "your_bright_data_api_token",
  "dataset_id": "gd_lyy3tktm25m4avu764",
  "scrape_url": "https://www.example.com"
}
```

#### 2. Run Debug Command

From the connector directory, run:

```bash
fivetran debug --configuration configuration.json
```

This command will:
- Validate your configuration
- Execute the connector's `schema` and `update` functions
- Create a local DuckDB database in the `files/` directory
- Display logs and execution results
- Allow you to inspect the data synced to the local database

#### 3. Inspect Local Database

The debug command creates a local DuckDB database at `files/warehouse.db`. You can query it using:

```bash
# Using DuckDB CLI
duckdb files/warehouse.db

# Then query the data
SELECT * FROM bright_data_scrape.scrape_results;
```

Or using Python:

```python
import duckdb

conn = duckdb.connect('files/warehouse.db')
results = conn.execute("SELECT * FROM bright_data_scrape.scrape_results").fetchall()
print(results)
```

#### 4. View Logs

The debug command outputs detailed logs including:
- Configuration validation
- API requests and responses
- Data processing steps
- Schema discovery updates
- Error messages (if any)

#### Debug Command Options

- `--configuration <file>`: Path to configuration JSON file (required)
- `--state <file>`: Path to state JSON file (optional, for incremental syncs)
- `--log-level <level>`: Set log level (DEBUG, INFO, WARNING, ERROR)

Example with custom state:

```bash
fivetran debug --configuration configuration.json --state files/state.json
```

### Deploying to Fivetran

Once you've tested your connector locally and verified it works correctly, you can deploy it to Fivetran.

#### 1. Prerequisites for Deployment

- A Fivetran account with connector development access
- The Fivetran CLI installed and authenticated
- Your connector code is ready for production

#### 2. Deploy Command

From the connector directory, run:

```bash
fivetran deploy --api-key <BASE_64_ENCODED_API_KEY> --destination <DESTINATION_NAME> --connection <CONNECTION_NAME> --configuration configuration.json
```

This command will:
- Package your connector code
- Upload it to Fivetran's infrastructure
- Make it available for use in your Fivetran account

#### 3. Configure Connector in Fivetran Dashboard

After deployment:

1. Go to your Fivetran dashboard
2. Navigate to **Connectors** → **Add Connector**
3. Find your connector in the list (it will appear with your connector name)
4. Configure the connector with your Bright Data credentials:
   - `api_token`: Your Bright Data API token
   - `dataset_id`: Your Bright Data dataset ID
   - `scrape_url`: The URL(s) to scrape
5. Save and test the connection

#### 4. Monitor Connector Syncs

Once deployed:
- Monitor sync status in the Fivetran dashboard
- View sync logs for debugging
- Check data in your destination warehouse
- Set up sync schedules as needed

### Best Practices

1. **Always test locally first**: Use `fivetran debug` to verify your connector works before deploying
2. **Check logs carefully**: Review debug logs to catch any issues early
3. **Validate configuration**: Ensure all required parameters are provided and correctly formatted
4. **Test with sample data**: Start with a small number of URLs to verify functionality
5. **Monitor first syncs**: After deployment, closely monitor the first few syncs to ensure everything works correctly
6. **Handle errors gracefully**: Ensure your connector has proper error handling and logging

### Common Debugging Issues

- **Configuration errors**: Verify all required fields are present and have correct types (all strings)
- **API authentication**: Ensure your `api_token` is valid and has necessary permissions
- **Database connection**: If using local DuckDB, ensure the `files/` directory exists and is writable
- **Import errors**: Verify all dependencies are installed and paths are correct
- **Timeout issues**: For long-running scrapes, ensure your local environment can handle extended execution times
