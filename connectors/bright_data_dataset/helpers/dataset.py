"""Bright Data Marketplace Dataset API helper functions."""

import json
import time
from typing import Any, Dict, List, Optional, Union

import requests
# Bright Data SDK for snapshot polling and downloading
from brightdata import bdclient
from requests import RequestException

from fivetran_connector_sdk import Logging as log

from .common import (BRIGHT_DATA_BASE_URL, DEFAULT_TIMEOUT_SECONDS,
                     MAX_SNAPSHOT_WAIT_SECONDS, RETRY_STATUS_CODES,
                     SNAPSHOT_POLL_INTERVAL_SECONDS, extract_error_detail,
                     parse_response_payload)


def filter_dataset(
    client: bdclient,
    api_token: str,
    dataset_id: str,
    filter_obj: Union[Dict[str, Any], str],
    records_limit: Optional[int] = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    retries: int = 3,
    backoff_factor: float = 1.5,
) -> List[Dict[str, Any]]:
    """
    Filter a Bright Data dataset and retrieve the filtered records.

    This function creates a snapshot by filtering the dataset via REST API,
    then uses the Bright Data SDK to poll for snapshot completion and retrieve
    the filtered records. The SDK handles polling automatically.

    Args:
        client: Initialized Bright Data client instance for snapshot polling
        api_token: Bright Data API token
        dataset_id: ID of the dataset to filter
        filter_obj: Filter object (dict or JSON string) containing filter criteria
        records_limit: Maximum number of records to include in the snapshot
        timeout: Request timeout in seconds
        retries: Number of retries for failed requests
        backoff_factor: Backoff factor for exponential backoff

    Returns:
        List of filtered dataset records

    Raises:
        ValueError: If API token, dataset_id, or filter is invalid
        RuntimeError: If snapshot creation or retrieval fails
    """
    if not api_token or not isinstance(api_token, str):
        raise ValueError("A valid Bright Data API token is required")

    if not dataset_id or not isinstance(dataset_id, str):
        raise ValueError("dataset_id must be a non-empty string")

    # Parse filter if it's a string
    if isinstance(filter_obj, str):
        try:
            filter_dict = json.loads(filter_obj)
        except json.JSONDecodeError as e:
            raise ValueError(f"filter must be valid JSON: {str(e)}") from e
    else:
        filter_dict = filter_obj

    if not isinstance(filter_dict, dict):
        raise ValueError("filter must be a dictionary or valid JSON string")

    # Create snapshot by filtering the dataset via REST API
    snapshot_id = _create_filtered_snapshot(
        api_token=api_token,
        dataset_id=dataset_id,
        filter_obj=filter_dict,
        records_limit=records_limit,
        timeout=timeout,
        retries=retries,
        backoff_factor=backoff_factor,
    )

    if not snapshot_id:
        raise RuntimeError("Failed to create filtered snapshot")

    # Use Bright Data SDK to poll for snapshot completion and retrieve records
    # The SDK's download_snapshot handles polling automatically
    records = _poll_and_get_snapshot_content(
        client=client,
        snapshot_id=snapshot_id,
        max_attempts=MAX_SNAPSHOT_WAIT_SECONDS // SNAPSHOT_POLL_INTERVAL_SECONDS,
        poll_interval=SNAPSHOT_POLL_INTERVAL_SECONDS,
    )

    log.info(f"Retrieved {len(records)} records from filtered dataset snapshot")
    return records


def _create_filtered_snapshot(
    api_token: str,
    dataset_id: str,
    filter_obj: Dict[str, Any],
    records_limit: Optional[int],
    timeout: int,
    retries: int,
    backoff_factor: float,
) -> str:
    """Create a filtered snapshot and return the snapshot_id."""
    url = f"{BRIGHT_DATA_BASE_URL}/datasets/filter"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    payload: Dict[str, Any] = {
        "dataset_id": dataset_id,
        "filter": filter_obj,
    }

    if records_limit is not None:
        payload["records_limit"] = records_limit

    attempt = 0
    backoff = backoff_factor

    while attempt <= retries:
        try:
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=timeout,
            )

            if response.status_code == 200:
                result = parse_response_payload(response)
                if isinstance(result, dict):
                    snapshot_id = result.get("snapshot_id")
                    if snapshot_id:
                        log.info(f"Created filtered snapshot: {snapshot_id[:8]}...")
                        return str(snapshot_id)
                    raise RuntimeError("Response missing snapshot_id")

            # Handle specific error status codes with appropriate error messages
            if response.status_code == 400:
                # Bad Request - validation errors
                error_detail = extract_error_detail(response)
                log.info(
                    f"Bright Data filter request validation failed: {error_detail}"
                )
                raise ValueError(f"Invalid request parameters: {error_detail}")

            if response.status_code == 402:
                # Payment Required - insufficient balance
                error_detail = extract_error_detail(response)
                log.info(f"Bright Data filter request payment failed: {error_detail}")
                raise RuntimeError(f"Insufficient account balance: {error_detail}")

            if response.status_code == 422:
                # Unprocessable Entity - filter didn't match any records
                error_detail = extract_error_detail(response)
                log.info(f"Bright Data filter request no matches: {error_detail}")
                raise ValueError(f"Filter did not match any records: {error_detail}")

            if response.status_code == 429:
                # Too Many Requests - rate limit exceeded
                error_detail = extract_error_detail(response)
                log.info(f"Bright Data filter request rate limited: {error_detail}")
                # Retry for 429 errors (they're in RETRY_STATUS_CODES)
                if attempt < retries:
                    log.info(
                        f"Bright Data filter request retry {attempt + 1}/{retries} "
                        f"(status code: {response.status_code})"
                    )
                    attempt += 1
                    time.sleep(backoff)
                    backoff *= backoff_factor
                    continue
                raise RuntimeError(f"Rate limit exceeded: {error_detail}")

            # Retry for other retryable status codes
            if response.status_code in RETRY_STATUS_CODES and attempt < retries:
                log.info(
                    f"Bright Data filter request retry {attempt + 1}/{retries} "
                    f"(status code: {response.status_code})"
                )
                attempt += 1
                time.sleep(backoff)
                backoff *= backoff_factor
                continue

            # Handle other non-retryable errors
            error_detail = extract_error_detail(response)
            log.info(f"Bright Data filter request failed: {error_detail}")
            response.raise_for_status()

        except RequestException as exc:
            if attempt < retries:
                log.info(
                    f"Error contacting Bright Data API: {str(exc)}. "
                    f"Retrying ({attempt + 1}/{retries})"
                )
                attempt += 1
                time.sleep(backoff)
                backoff *= backoff_factor
                continue
            raise RuntimeError(
                f"Failed to create filtered snapshot after {retries} retries: {str(exc)}"
            ) from exc

    raise RuntimeError("Failed to create filtered snapshot after retries")


def _poll_and_get_snapshot_content(
    client: bdclient,
    snapshot_id: str,
    max_attempts: int,
    poll_interval: int,
) -> List[Dict[str, Any]]:
    """
    Poll snapshot using Bright Data SDK and retrieve its content when ready.

    Uses the SDK's download_snapshot function which handles polling automatically.
    The SDK polls until the snapshot is ready or fails.

    Args:
        client: Initialized Bright Data client instance
        snapshot_id: ID of the snapshot to poll
        max_attempts: Maximum number of polling attempts
        poll_interval: Interval between polling attempts in seconds

    Returns:
        List of dataset records from the snapshot.
        If polling or downloading fails, returns a list with a single dict
        containing error information: {"snapshot_id": str, "status": "failed",
        "error": str, "error_type": str}
    """
    attempt = 0

    while attempt < max_attempts:
        try:
            # Use SDK's download_snapshot to check status and get data when ready
            # Following the same pattern as scrape.py
            snapshot_response = client.download_snapshot(snapshot_id)

            status = None
            snapshot_data = None

            if isinstance(snapshot_response, dict):
                status = snapshot_response.get("status", "").lower()
                if status == "ready":
                    # Snapshot is ready, extract the data from the response
                    # For dataset snapshots, data might be in "data", "records", or "results" keys
                    # Or the entire response might be the data (excluding metadata)
                    snapshot_data = (
                        snapshot_response.get("data") or snapshot_response.get("records") or snapshot_response.get("results")
                    )

                    # If no data key, check if the entire response is the data
                    if snapshot_data is None:
                        # Remove status/metadata fields and use remaining fields as data
                        metadata_keys = (
                            "status",
                            "id",
                            "snapshot_id",
                            "created",
                            "dataset_id",
                            "customer_id",
                            "cost",
                            "initiation_type",
                            "warning",
                            "warning_code",
                        )
                        snapshot_data = {
                            k: v
                            for k, v in snapshot_response.items()
                            if k not in metadata_keys
                        }
                    # If still None, use the entire response as data
                    if snapshot_data is None:
                        snapshot_data = snapshot_response
                elif status == "failed":
                    # Extract error message from multiple possible fields
                    error_msg = None

                    # Check standard error fields including 'warning' (used by Bright Data API)
                    for key in ("error", "warning", "message", "detail", "details"):
                        if key in snapshot_response:
                            error_value = snapshot_response[key]
                            if error_value:
                                error_msg = str(error_value)
                                break

                    # Check warning_code as fallback
                    if not error_msg and "warning_code" in snapshot_response:
                        warning_code = snapshot_response.get("warning_code")
                        error_msg = f"Snapshot failed with warning code: {warning_code}"

                    # If still no error message, use the full response
                    if not error_msg:
                        error_msg = f"Unknown error. Full response: {snapshot_response}"

                    # Log the full response for debugging
                    log.info(
                        f"Snapshot {snapshot_id[:8]}... failed. "
                        f"Response: {snapshot_response}"
                    )

                    # Return failed message as content instead of raising exception
                    failed_message = {
                        "snapshot_id": snapshot_id,
                        "status": "failed",
                        "error": error_msg,
                        "error_type": "snapshot_failed",
                    }
                    return [failed_message]
                elif status in ("running", "pending", "processing", "scheduled"):
                    log.info(
                        f"Snapshot {snapshot_id[:8]}... status: {status} "
                        f"(attempt {attempt + 1}/{max_attempts})"
                    )
                else:
                    log.info(
                        f"Snapshot {snapshot_id[:8]}... status: {status} "
                        f"(attempt {attempt + 1}/{max_attempts})"
                    )
            elif snapshot_response:
                # SDK returned data directly (not wrapped in status dict)
                snapshot_data = snapshot_response
                status = "ready"

            # If snapshot is ready and we have data, normalize and return it
            if snapshot_data:
                # Normalize to list - dataset snapshots return a list of records
                if isinstance(snapshot_data, list):
                    return snapshot_data
                elif isinstance(snapshot_data, dict):
                    # If single dict, wrap in list (single record)
                    return [snapshot_data] if snapshot_data else []
                else:
                    # If other type, wrap in list
                    return [snapshot_data]

            # If response is None or empty, snapshot may still be processing
            attempt += 1
            if attempt < max_attempts:
                time.sleep(poll_interval)

        except Exception as e:
            error_msg = str(e)
            error_msg_lower = error_msg.lower()
            error_type = type(e).__name__

            # Check for 404 - Snapshot does not exist (don't retry, return immediately)
            if (
                "404" in error_msg_lower or "not found" in error_msg_lower or "does not exist" in error_msg_lower
            ):
                failed_message = {
                    "snapshot_id": snapshot_id,
                    "status": "failed",
                    "error": f"Snapshot not found: {error_msg}",
                    "error_type": "snapshot_not_found",
                    "exception_type": error_type,
                }
                log.info(
                    f"Snapshot {snapshot_id[:8]}... not found. Returning failed message."
                )
                return [failed_message]

            # Log the error with full context for other errors
            log.info(
                f"Error polling snapshot {snapshot_id[:8]}... ({error_type}): {error_msg}. "
                f"Retrying (attempt {attempt + 1}/{max_attempts})"
            )

            attempt += 1
            if attempt < max_attempts:
                time.sleep(poll_interval)
            else:
                # If we've exhausted retries, return failed message as content
                failed_message = {
                    "snapshot_id": snapshot_id,
                    "status": "failed",
                    "error": f"Failed to poll/download snapshot after {max_attempts} attempts: {error_msg}",
                    "error_type": "polling_failed",
                    "exception_type": error_type,
                }
                log.info(
                    f"Snapshot {snapshot_id[:8]}... polling failed after {max_attempts} attempts"
                )
                return [failed_message]

    # Timeout - snapshot did not complete within max attempts
    failed_message = {
        "snapshot_id": snapshot_id,
        "status": "failed",
        "error": f"Snapshot did not complete within {max_attempts * poll_interval} seconds",
        "error_type": "timeout",
    }
    log.info(f"Snapshot {snapshot_id[:8]}... timed out after {max_attempts} attempts")
    return [failed_message]
