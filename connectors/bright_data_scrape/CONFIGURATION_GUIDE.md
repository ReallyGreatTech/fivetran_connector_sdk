# Bright Data Web Scraper Connector Configuration Guide

## Overview

This connector fetches data from Bright Data's Web Scraper API and syncs the scraped results to your Fivetran destination. The connector automatically handles asynchronous job processing and dynamically discovers schema fields from the scraped data.

**Important**: All configuration values must be provided as **strings** per Fivetran SDK requirements.

## Configuration Parameters

### Required Parameters

- **`api_token`** (string, required): Your Bright Data API token.
  - Get your API token from: https://brightdata.com/cp/setting/users
  - Format: Bearer token (the connector will automatically add the "Bearer " prefix)

- **`dataset_id`** (string, required): The ID of the Bright Data dataset to use for scraping.
  - Get your dataset ID from the Bright Data dashboard
  - Example: `"gd_lyy3tktm25m4avu764"`
  - Each dataset has specific configuration requirements - check your dataset documentation

- **`scrape_url`** (string, required): The URL(s) to scrape.
  - Can be a single URL, comma-separated URLs, newline-separated URLs, or JSON array string
  - Example: `"https://www.example.com"`
  - See [URL Input Formats](#url-input-formats) section below for supported formats


## URL Input Formats

The `scrape_url` parameter supports multiple input formats for flexibility:

### 1. Single URL

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_lyy3tktm25m4avu764",
  "scrape_url": "https://www.example.com"
}
```

### 2. Comma-Separated URLs (Recommended)

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_lyy3tktm25m4avu764",
  "scrape_url": "https://www.example.com,https://www.example2.com,https://www.example3.com"
}
```

### 3. Newline-Separated URLs (Great for Text Areas)

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_lyy3tktm25m4avu764",
  "scrape_url": "https://www.example.com\nhttps://www.example2.com\nhttps://www.example3.com"
}
```

### 4. JSON Array String (Programmatic)

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_lyy3tktm25m4avu764",
  "scrape_url": "[\"https://www.example.com\",\"https://www.example2.com\",\"https://www.example3.com\"]"
}
```

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
  "dataset_id": "gd_lyy3tktm25m4avu764",
  "scrape_url": "https://www.example.com"
}
```

### Multiple URLs Configuration

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_lyy3tktm25m4avu764",
  "scrape_url": "https://www.example.com,https://www.example2.com,https://www.example3.com"
}
```


## Troubleshooting

### Configuration Errors

- **Missing api_token**: Ensure `api_token` is provided as a string
- **Missing dataset_id**: Ensure `dataset_id` is provided as a string
- **Empty scrape_url**: Ensure `scrape_url` is provided and not empty
- **Invalid dataset_id**: Verify your dataset ID is correct and accessible in your Bright Data account

### Common Issues

- **URL not accessible**: Verify the URL is publicly accessible or accessible via Bright Data
- **Invalid input provided** (400/422 errors): Check your dataset configuration and ensure URLs are valid
- **Rate limiting** (429 errors): If you encounter rate limits, consider reducing the number of URLs or waiting before retrying
- **Snapshot failed**: Check the Bright Data dashboard for detailed error information about why the snapshot failed
- **Invalid API token**: Verify your API token is correct and has access to the Web Scraper API
- **Missing permissions**: Ensure your Bright Data account has access to the Web Scraper feature and the specific dataset
- **Insufficient balance** (402 errors): Add funds to your Bright Data account

## References

- [Bright Data Web Scraper Documentation](https://docs.brightdata.com/)
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connectors/connector-sdk)
