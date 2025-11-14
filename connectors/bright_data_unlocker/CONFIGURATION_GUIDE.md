# Bright Data Web Unlocker Connector Configuration Guide

## Overview

This connector fetches data from Bright Data's Web Unlocker REST API
and syncs the unlocked results to your Fivetran destination. The connector
dynamically creates tables with flattened dictionary structures,
allowing Fivetran to infer column types automatically.

The Web Unlocker API is designed to bypass geo-blocking, captchas, and other
access restrictions, making it ideal for accessing protected websites.

**Important**: All configuration values must be provided as **strings** per Fivetran SDK requirements.

## Configuration Parameters

### Required Parameters

- **`api_token`** (string, required): Your Bright Data API token.
  - Get your API token from: https://brightdata.com/cp/setting/users
  - Format: Bearer token (the connector will automatically add the "Bearer " prefix)

- **`unlocker_url`** (string, required): The URL(s) to unlock/fetch.
  - Can be a single URL, comma-separated URLs, newline-separated URLs, or JSON array string
  - Example: `"https://geo.brdtest.com/welcome.txt"`
  - See [URL Input Formats](#url-input-formats) section below for supported formats

### Optional Parameters

- **`zone`** (string, optional): The Bright Data zone identifier for Web Unlocker API.
  - Default: `"web_unlocker1"` (if not specified in configuration)
  - Example: `"web_unlocker1"`, `"web_unlocker2"`
  - Find your zone in the Bright Data dashboard

- **`country`** (string, optional): Country code for geolocation targeting.
  - Use ISO 3166-1 alpha-2 country codes (lowercase)
  - Default: `"us"` if not provided
  - Example: `"us"`, `"gb"`, `"de"`, `"fr"`
  - Affects which IP addresses are used for unlocking

- **`method`** (string, optional): HTTP method to use for the request.
  - Default: `"GET"`
  - Example: `"GET"`, `"POST"`
  - If not provided, uses GET method

- **`format_param`** (string, optional): Output format for the response.
  - Default: `"json"`
  - Example: `"json"`, `"html"`
  - Controls the response structure from Bright Data API

- **`data_format`** (string, optional): Format for extracted data.
  - Default: `"markdown"`
  - Example: `"markdown"`, `"html"`, `"text"`
  - Controls how the content is extracted and formatted

## URL Input Formats

The `unlocker_url` parameter supports multiple input formats for flexibility:

### 1. Single URL

```json
{
  "api_token": "your_api_token",
  "unlocker_url": "https://geo.brdtest.com/welcome.txt"
}
```

### 2. Comma-Separated URLs (Recommended)

```json
{
  "api_token": "your_api_token",
  "unlocker_url": "https://example.com/public,https://example.org/geo-unblocked,https://example.net/unrestricted"
}
```

### 3. Newline-Separated URLs (Great for Text Areas)

```json
{
  "api_token": "your_api_token",
  "unlocker_url": "https://example.com/public\nhttps://example.org/geo-unblocked\nhttps://example.net/unrestricted"
}
```

### 4. JSON Array String (Programmatic)

```json
{
  "api_token": "your_api_token",
  "unlocker_url": "[\"https://example.com/public\",\"https://example.org/geo-unblocked\",\"https://example.net/unrestricted\"]"
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
  "unlocker_url": "https://geo.brdtest.com/welcome.txt"
}
```

### Configuration with Country Targeting

```json
{
  "api_token": "your_api_token",
  "unlocker_url": "https://example.com/unprotected",
  "country": "gb"
}
```

### Configuration with Custom Zone

```json
{
  "api_token": "your_api_token",
  "unlocker_url": "https://example.com/unprotected",
  "zone": "web_unlocker1",
  "country": "us"
}
```

### Multiple URLs with Format Options

```json
{
  "api_token": "your_api_token",
  "unlocker_url": "https://example.com/page1,https://example.com/page2",
  "zone": "web_unlocker1",
  "country": "us",
  "data_format": "markdown",
  "format_param": "json",
  "method": "GET"
}
```

### Full Configuration Example

```json
{
  "api_token": "your_api_token",
  "unlocker_url": "https://example.com/unprotected,https://example.org/geo-unblocked",
  "zone": "web_unlocker1",
  "country": "us",
  "method": "GET",
  "format_param": "json",
  "data_format": "markdown"
}
```

### POST Method Configuration

```json
{
  "api_token": "your_api_token",
  "unlocker_url": "https://example.com/api/endpoint",
  "method": "POST",
  "country": "us"
}
```

## How It Works

1. **Configuration Validation**: The connector validates that `api_token` and `unlocker_url` are provided
2. **URL Normalization**: The connector parses and normalizes the `unlocker_url` input into a list of URLs
3. **Batch Processing**: URLs are processed via Bright Data's Web Unlocker REST API (supports batch processing)
4. **API Requests**: Each URL is sent as a POST request to `/request` endpoint with retry logic
5. **Unlocking**: Bright Data handles geo-blocking, captchas, and other access restrictions
6. **Data Processing**: Unlocked results are flattened and processed
7. **Schema Discovery**: Fields are dynamically discovered and documented in `fields.yaml`
8. **Data Upsertion**: Processed records are upserted to the `unlocker_results` table
9. **State Checkpointing**: Progress is checkpointed to enable resume on interruption

## Notes

- **Batch Processing**: Multiple URLs are processed efficiently, maintaining URL-to-result order
- **Error Handling**: If one URL fails, the connector continues processing other URLs
- **Schema Discovery**: The connector uses dynamic schema discovery - only primary keys (`requested_url`, `result_index`) are defined in the schema
- **Field Documentation**: All discovered fields are automatically documented in `fields.yaml`
- **Retry Logic**: The connector includes automatic retry logic with exponential backoff for transient errors
- **Data Flattening**: Nested dictionaries and lists in unlocker results are automatically flattened with underscore separators
- **Result Mapping**: Results are matched to URLs by index order (maintained by the API)
- **Geo-Blocking**: The Web Unlocker automatically handles geo-restrictions based on the `country` parameter

## Troubleshooting

### Configuration Errors

- **Missing api_token**: Ensure `api_token` is provided as a string
- **Empty unlocker_url**: Ensure `unlocker_url` is provided and not empty
- **Invalid country code**: Use valid ISO 3166-1 alpha-2 country codes (lowercase)

### API Errors

- **Invalid API token**: Verify your API token is correct and has access to the Web Unlocker API
- **Zone not found**: Ensure your `zone` matches a valid zone in your Bright Data account
- **Rate limiting**: If you encounter rate limits, consider reducing the number of URLs per sync
- **429 errors**: The connector automatically retries on rate limit errors with exponential backoff

### Unlocking Errors

- **URL not accessible**: Verify the URL is valid and accessible via Bright Data's unlocker
- **Geo-blocking issues**: Ensure the `country` parameter matches your needs
- **Timeout errors**: Check your network connection and Bright Data service status
- **CAPTCHA failures**: Some sites may require manual intervention or different settings

## References

- [Bright Data Web Unlocker API Documentation](https://docs.brightdata.com/api-reference/rest-api/unlocker/unlock-website)
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connectors/connector-sdk)
