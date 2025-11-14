# Bright Data SERP Connector Configuration Guide

## Overview

This connector fetches search engine results from Bright Data's SERP REST API
and syncs the results to your Fivetran destination. The connector
dynamically creates tables with flattened dictionary structures,
allowing Fivetran to infer column types automatically.

**Important**: All configuration values must be provided as **strings** per Fivetran SDK requirements.

## Configuration Parameters

### Required Parameters

- **`api_token`** (string, required): Your Bright Data API token.
  - Get your API token from: https://brightdata.com/cp/setting/users
  - Format: Bearer token (the connector will automatically add the "Bearer " prefix)

- **`search_query`** (string, required): The search query/queries to execute.
  - Can be a single query, comma-separated queries, newline-separated queries, or JSON array string
  - Example: `"python tutorial"` or `"query1,query2,query3"`
  - See [Query Input Formats](#query-input-formats) section below for supported formats

### Optional Parameters

- **`search_engine`** (string, optional): The search engine to use.
  - Default: `"google"`
  - Valid values: `"google"`, `"bing"`, `"yandex"`
  - Case-insensitive
  - If not provided or invalid, defaults to `"google"`

- **`search_zone`** (string, optional): The Bright Data zone identifier for SERP API.
  - Also accepts `"zone"` as an alias
  - Default: `"serp_api1"` (if not specified in configuration)
  - Example: `"serp_api1"`, `"serp_api2"`
  - Find your zone in the Bright Data dashboard

- **`country`** (string, optional): Country code for geolocation targeting.
  - Use ISO 3166-1 alpha-2 country codes (lowercase)
  - Example: `"us"`, `"gb"`, `"de"`, `"fr"`
  - Default: `"us"` if not provided
  - Affects search results localization

- **`format`** (string, optional): Output format for the response.
  - Default: `"json"`
  - Example: `"json"`, `"html"`
  - Controls the response structure from Bright Data API

## Query Input Formats

The `search_query` parameter supports multiple input formats for flexibility:

### 1. Single Query

```json
{
  "api_token": "your_api_token",
  "search_query": "python tutorial"
}
```

### 2. Comma-Separated Queries (Recommended)

```json
{
  "api_token": "your_api_token",
  "search_query": "python tutorial,data science,web scraping"
}
```

### 3. Newline-Separated Queries (Great for Text Areas)

```json
{
  "api_token": "your_api_token",
  "search_query": "python tutorial\ndata science\nweb scraping"
}
```

### 4. JSON Array String (Programmatic)

```json
{
  "api_token": "your_api_token",
  "search_query": "[\"python tutorial\",\"data science\",\"web scraping\"]"
}
```

### Parsing Order

The connector processes queries in the following order:

1. **JSON parsing**: Attempts to parse as JSON first (supports arrays and single strings)
2. **Comma separation**: If JSON parsing fails, splits on commas
3. **Newline separation**: If no commas found, splits on newlines
4. **Single value**: Otherwise treats the entire input as a single query

## Example Configurations

### Basic Configuration

```json
{
  "api_token": "your_api_token",
  "search_query": "python tutorial"
}
```

### Configuration with Search Engine

```json
{
  "api_token": "your_api_token",
  "search_query": "python tutorial",
  "search_engine": "bing"
}
```

### Configuration with Country Targeting

```json
{
  "api_token": "your_api_token",
  "search_query": "python tutorial",
  "country": "gb"
}
```

### Multiple Queries with Custom Zone

```json
{
  "api_token": "your_api_token",
  "search_query": "python tutorial,data science,machine learning",
  "search_engine": "google",
  "search_zone": "serp_api1",
  "country": "us"
}
```

### Full Configuration Example

```json
{
  "api_token": "your_api_token",
  "search_query": "python tutorial,data science",
  "search_engine": "google",
  "search_zone": "serp_api1",
  "country": "us",
  "format": "json"
}
```

### Yandex Search Engine Example

```json
{
  "api_token": "your_api_token",
  "search_query": "python обучение",
  "search_engine": "yandex",
  "country": "ru"
}
```

## How It Works

1. **Configuration Validation**: The connector validates that `api_token` and `search_query` are provided
2. **Query Normalization**: The connector parses and normalizes the `search_query` input into a list of queries
3. **Batch Processing**: Queries are executed via Bright Data's SERP REST API (supports batch processing)
4. **API Requests**: Each query is sent as a POST request to `/request` endpoint with retry logic
5. **Data Processing**: Search results are flattened and processed
6. **Schema Discovery**: Fields are dynamically discovered and documented in `fields.yaml`
7. **Data Upsertion**: Processed records are upserted to the `search_results` table
8. **State Checkpointing**: Progress is checkpointed to enable resume on interruption

## Notes

- **Batch Processing**: Multiple queries are processed efficiently, maintaining query-to-result order
- **Error Handling**: If one query fails, the connector continues processing other queries
- **Schema Discovery**: The connector uses dynamic schema discovery - only primary keys (`query`, `result_index`) are defined in the schema
- **Field Documentation**: All discovered fields are automatically documented in `fields.yaml`
- **Retry Logic**: The connector includes automatic retry logic with exponential backoff for transient errors
- **Data Flattening**: Nested dictionaries and lists in search results are automatically flattened with underscore separators
- **Search Engine Support**: Currently supports Google, Bing, and Yandex search engines
- **Result Mapping**: Results are matched to queries by index order (maintained by the API)

## Troubleshooting

### Configuration Errors

- **Missing api_token**: Ensure `api_token` is provided as a string
- **Empty search_query**: Ensure `search_query` is provided and not empty
- **Invalid search_engine**: Use one of the supported engines: `"google"`, `"bing"`, `"yandex"` (case-insensitive)
- **Invalid country code**: Use valid ISO 3166-1 alpha-2 country codes (lowercase)

### API Errors

- **Invalid API token**: Verify your API token is correct and has access to the SERP API
- **Zone not found**: Ensure your `search_zone` matches a valid zone in your Bright Data account
- **Rate limiting**: If you encounter rate limits, consider reducing the number of queries per sync
- **429 errors**: The connector automatically retries on rate limit errors with exponential backoff

### Search Errors

- **No results returned**: Verify the query syntax and that the search engine supports your query
- **Invalid query format**: Ensure queries are properly formatted strings
- **Timeout errors**: Check your network connection and Bright Data service status

## References

- [Bright Data SERP API Documentation](https://docs.brightdata.com/api-reference/rest-api/serp/scrape-serp)
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connectors/connector-sdk)

