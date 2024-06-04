import time
import requests
import json
import ffmpeg
import os
import base64
import tempfile


class NoOutputFromRunpodException(Exception):
    """Exception raised when there is no output from Runpod."""


def check_health(api_key, server_endpoint):
    """
    Checks health and worker statistics of a particular endpoint.

    Args:
        api_key (str): Runpod API key.
        server_endpoint (str): Server endpoint.

    Returns:
        dict: Health statistics response from Runpod.
    """
    url = f"https://api.runpod.ai/v2/{server_endpoint}/health"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    response = requests.get(url, headers=headers).json()
    return response


def cancel_job(job_id, api_key, server_endpoint):
    """
    Cancels a transcription job given its job ID.

    Args:
        job_id (str): Job ID of the transcription request to cancel.
        api_key (str): Runpod API key.
        server_endpoint (str): Server endpoint.

    Returns:
        dict: Cancellation response from Runpod.
    """
    url = f"https://api.runpod.ai/v2/{server_endpoint}/cancel/{job_id}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    response = requests.post(url, headers=headers).json()
    return response


def send_async_rqeuest_to_runpod(
    payload_request, api_key, server_endpoint, execution_timeout=600000
):
    """
    Sends an asynchronous transcription request to Runpod.

    Args:
        base64_string_or_url (str): Base64-encoded audio data or a URL that starts with "http".
        api_key (str): Runpod API key.
        server_endpoint (str): Server endpoint.
        execution_timeout (int): Execution timeout in milliseconds, default is 600,000 (10 minutes).

    Returns:
        str: Job ID of the transcription request.
    """
    url = f"https://api.runpod.ai/v2/{server_endpoint}/run"


    policy = {"executionTimeout": execution_timeout}

    endpoint_payload = json.dumps({"input": payload_request, "policy": policy})

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    response = requests.post(url, headers=headers, data=endpoint_payload).json()
    print(f'Runpod Job ID: ${response["id"]}')
    return response["id"]


def get_endpoint_status(job_id, api_key, server_endpoint):
    """
    Gets the status of a transcription job from Runpod.

    Args:
        job_id (str): Job ID of the transcription request.
        api_key (str): Runpod API key.
        server_endpoint (str): Server endpoint.

    Returns:
        dict: Status response from Runpod.
    """
    url = f"https://api.runpod.ai/v2/{server_endpoint}/status/{job_id}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    response = requests.get(url, headers=headers).json()
    return response


def wait_for_job_to_complete(
    job_id, api_key, server_endpoint, polling_interval=20
):
    """
    Waits for the transcription job to complete and returns the output.

    Args:
        job_id (str): Job ID of the transcription request.
        api_key (str): Runpod API key.
        server_endpoint (str): Server endpoint.
        sleep_interval (int, optional): Time in seconds to sleep between status checks. Default is 20 seconds.

    Returns:
        dict: Transcription output or status.
    """
    while True:
        status_response = get_endpoint_status(job_id, api_key, server_endpoint)
        status = status_response["status"]

        if status in ["IN_PROGRESS", "IN_QUEUE"]:
            print("Job still in progress...", status)
            time.sleep(polling_interval)
        else:
            if status == "COMPLETED":
                print("Job completed!")
                print(status_response.get("output"))
                return {
                    "status": "COMPLETED",
                    "output": status_response.get("output"),
                }
            else:
                raise NoOutputFromRunpodException(
                    f"Job failed with status: {status}"
                )


def runpod_api_request_and_poll(
    payload, runpod_api_key, server_endpoint, polling_interval=20
):
    """
    Transcribes audio using Runpod's API.

    Args:
        base64_string_or_url (str): Base64-encoded audio data or a URL that starts with "http".
        api_key (str): Runpod API key.
        server_endpoint (str): Server endpoint.

    Returns:
        dict: Transcription output or status.
    """
    job_id = send_async_rqeuest_to_runpod(
        payload, runpod_api_key, server_endpoint
    )
    return wait_for_job_to_complete(
        job_id, runpod_api_key, server_endpoint, polling_interval
    )

# Example call usage
# text = "Hello world, how are you doing today?"
# base_input = {"text": text}
# output = runpod_api_request_and_poll(base_input, "API KEY", "ENDPOINT", 3)
# print(output)