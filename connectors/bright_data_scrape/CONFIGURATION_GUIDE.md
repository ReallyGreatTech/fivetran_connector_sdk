# Bright Data Web Scraper Connector Configuration Guide

## Overview

This connector fetches data from Bright Data's Web Scraper API using the Bright Data SDK
and syncs the scraped results to your Fivetran destination. The connector
dynamically creates tables with flattened dictionary structures,
allowing Fivetran to infer column types automatically.

**Important**: All configuration values must be provided as **strings** per Fivetran SDK requirements.

## Configuration Parameters

### Required Parameters

- **`api_token`** (string, required): Your Bright Data API token.
  - Get your API token from: https://brightdata.com/cp/setting/users
  - Format: Bearer token (the connector will automatically add the "Bearer " prefix)

- **`scrape_url`** (string, required): The URL(s) to scrape.
  - Can be a single URL, comma-separated URLs, newline-separated URLs, or JSON array string
  - Example: `"https://www.example.com"`
  - See [URL Input Formats](#url-input-formats) section below for supported formats

### Optional Parameters

- **`country`** (string, optional): Country code for geolocation targeting.
  - Use ISO 3166-1 alpha-2 country codes (lowercase)
  - Example: `"us"`, `"gb"`, `"de"`
  - If not provided, Bright Data uses default geolocation

- **`data_format`** (string, optional): Format for extracted data.
  - Example: `"json"`, `"html"`, `"markdown"`
  - If not provided, uses Bright Data default

- **`format`** (string, optional): Output format for the response.
  - Default: `"json"`
  - Example: `"json"`, `"html"`
  - Note: Different from `data_format` - controls the response structure

- **`method`** (string, optional): HTTP method to use for the request.
  - Default: `"GET"`
  - Example: `"GET"`, `"POST"`
  - If not provided, uses GET method

- **`async_request`** (string, optional): Whether to use asynchronous requests.
  - Default: `"true"` (asynchronous)
  - Valid values: `"true"`, `"false"`, `"1"`, `"0"`, `"yes"`, `"no"`
  - Asynchronous requests are recommended for better performance with multiple URLs

## URL Input Formats

The `scrape_url` parameter supports multiple input formats for flexibility:

### 1. Single URL

```json
{
  "api_token": "your_api_token",
  "scrape_url": "https://www.example.com"
}
```

### 2. Comma-Separated URLs (Recommended)

```json
{
  "api_token": "your_api_token",
  "scrape_url": "https://www.example.com,https://www.example2.com,https://www.example3.com"
}
```

### 3. Newline-Separated URLs (Great for Text Areas)

```json
{
  "api_token": "your_api_token",
  "scrape_url": "https://www.example.com\nhttps://www.example2.com\nhttps://www.example3.com"
}
```

### 4. JSON Array String (Programmatic)

```json
{
  "api_token": "your_api_token",
  "scrape_url": "[\"https://www.example.com\",\"https://www.example2.com\",\"https://www.example3.com\"]"
}
```

### Parsing Order

The connector processes URLs in the following order:

1. **JSON parsing**: Attempts to parse as JSON first (supports arrays and single strings)
2. **Comma separation**: If JSON parsing fails, splits on commas
3. **Newline separation**: If no commas found, splits on newlines
4. **Single value**: Otherwise treats the entire input as a single URL

## Example Configurations

### Basic Configuration

```json
{
  "api_token": "your_api_token",
  "scrape_url": "https://www.example.com"
}
```

### Configuration with Country Targeting

```json
{
  "api_token": "your_api_token",
  "scrape_url": "https://www.example.com",
  "country": "us"
}
```

### Configuration with Data Format

```json
{
  "api_token": "your_api_token",
  "scrape_url": "https://www.example.com",
  "data_format": "json",
  "format": "json"
}
```

### Multiple URLs with Async Requests

```json
{
  "api_token": "your_api_token",
  "scrape_url": "https://www.example.com,https://www.example2.com,https://www.example3.com",
  "country": "us",
  "async_request": "true"
}
```

### Synchronous Request Configuration

```json
{
  "api_token": "your_api_token",
  "scrape_url": "https://www.example.com",
  "async_request": "false"
}
```

### POST Method Configuration

```json
{
  "api_token": "your_api_token",
  "scrape_url": "https://www.example.com/api/endpoint",
  "method": "POST",
  "data_format": "json"
}
```

## How It Works

1. **Configuration Validation**: The connector validates that `api_token` and `scrape_url` are provided
2. **URL Normalization**: The connector parses and normalizes the `scrape_url` input into a list of URLs
3. **Scraping**: URLs are scraped using the Bright Data SDK (supports both sync and async modes)
4. **Async Processing**: For async requests, the connector polls for snapshot completion
5. **Data Processing**: Scraped results are flattened and processed
6. **Schema Discovery**: Fields are dynamically discovered and documented in `fields.yaml`
7. **Data Upsertion**: Processed records are upserted to the `scrape_results` table
8. **State Checkpointing**: Progress is checkpointed to enable resume on interruption

## Notes

- **Async vs Sync**: Async requests are recommended for better performance and are enabled by default
- **Error Handling**: If one URL fails, the connector continues processing other URLs
- **Schema Discovery**: The connector uses dynamic schema discovery - only primary keys (`url`, `result_index`) are defined in the schema
- **Field Documentation**: All discovered fields are automatically documented in `fields.yaml`
- **Snapshot Polling**: For async requests, the connector automatically polls for snapshot completion (up to 60 attempts with 5-second intervals)
- **Data Flattening**: Nested dictionaries and lists are automatically flattened with underscore separators

## Troubleshooting

### Configuration Errors

- **Missing api_token**: Ensure `api_token` is provided as a string
- **Empty scrape_url**: Ensure `scrape_url` is provided and not empty
- **Invalid country code**: Use valid ISO 3166-1 alpha-2 country codes (lowercase)

### Scraping Errors

- **URL not accessible**: Verify the URL is publicly accessible or accessible via Bright Data
- **Snapshot timeout**: For async requests, if snapshots don't complete within the timeout period, check Bright Data dashboard
- **Rate limiting**: If you encounter rate limits, consider reducing the number of URLs or using async requests with longer polling intervals

### API Errors

- **Invalid API token**: Verify your API token is correct and has access to the Web Scraper API
- **Missing permissions**: Ensure your Bright Data account has access to the Web Scraper feature

## References

- [Bright Data Web Scraper Documentation](https://docs.brightdata.com/)
- [Bright Data Python SDK](https://github.com/brightdata/brightdata-python)
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connectors/connector-sdk)

