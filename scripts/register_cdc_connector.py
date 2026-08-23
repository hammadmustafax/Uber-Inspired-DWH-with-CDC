"""
=============================================================================
Uber Lakehouse Platform - CDC Connector Registration Script
=============================================================================
Description: Registers the PostgreSQL Debezium CDC connector with Kafka Connect
             REST API. Utilizes standard library urllib to avoid extra package
             dependencies.
=============================================================================
"""

import json
import os
import time
import urllib.error
import urllib.request

DEBEZIUM_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "uber-postgres-connector"
CONFIG_FILE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "src",
    "cdc_connectors",
    "postgres_cdc_connector.json"
)


def wait_for_debezium(max_attempts=12, delay_sec=5):
    """
    Polls Debezium API root to verify the service is running before registering.
    """
    print("[Debezium] Waiting for REST API service to start...")
    for attempt in range(1, max_attempts + 1):
        try:
            with urllib.request.urlopen("http://localhost:8083/", timeout=3) as response:
                if response.status == 200:
                    print("[Debezium] REST API is up and healthy.")
                    return True
        except (urllib.error.URLError, TimeoutError):
            print(f"[Debezium] Attempt {attempt}/{max_attempts}: Service not ready yet. Retrying in {delay_sec}s...")
            time.sleep(delay_sec)
    
    print("[Debezium Error] Timeout waiting for REST API to start.")
    return False


def is_connector_registered():
    """
    Checks if our connector name is already present on the Debezium server.
    """
    url = f"{DEBEZIUM_URL}/{CONNECTOR_NAME}"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=3) as response:
            if response.status == 200:
                return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
    except Exception:
        pass
    return False


def register_connector(payload):
    """
    Sends POST request to Kafka Connect REST API to create the connector.
    """
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        DEBEZIUM_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    
    try:
        with urllib.request.urlopen(req) as response:
            if response.status == 201:
                print(f"[Debezium] Connector '{CONNECTOR_NAME}' successfully registered.")
                return True
    except urllib.error.HTTPError as e:
        error_msg = e.read().decode("utf-8")
        print(f"[Debezium Error] Registration failed: {e.code} - {error_msg}")
    except Exception as e:
        print(f"[Debezium Error] Failed to submit config: {e}")
    return False


def check_connector_status():
    """
    Retrieves and displays the running tasks status of our connector.
    """
    url = f"{DEBEZIUM_URL}/{CONNECTOR_NAME}/status"
    try:
        with urllib.request.urlopen(url) as response:
            status_data = json.loads(response.read().decode("utf-8"))
            connector_state = status_data.get("connector", {}).get("state", "UNKNOWN")
            tasks = status_data.get("tasks", [])
            
            print(f"\nConnector Status: {connector_state}")
            for t in tasks:
                print(f" - Task #{t.get('id')}: {t.get('state')} on worker {t.get('worker_id')}")
                if t.get("state") == "FAILED":
                    print(f"   Reason: {t.get('trace')}")
    except Exception as e:
        print(f"[Debezium Error] Failed to read status details: {e}")


def main():
    if not wait_for_debezium():
        print("[Abort] Debezium Connect service is not available. Please verify Docker containers.")
        return

    # Load configuration JSON
    try:
        with open(CONFIG_FILE_PATH, "r") as f:
            config_payload = json.load(f)
    except FileNotFoundError:
        print(f"[Abort] Connector config not found at path: {CONFIG_FILE_PATH}")
        return
    except json.JSONDecodeError as e:
        print(f"[Abort] Config file has invalid JSON structure: {e}")
        return

    # Execute registration flow
    if is_connector_registered():
        print(f"[Debezium] Connector '{CONNECTOR_NAME}' is already registered. Skipping creation.")
    else:
        print(f"[Debezium] Registering connector configuration...")
        register_connector(config_payload)
        time.sleep(2)  # Give Connect a brief moment to spin up tasks
    
    check_connector_status()


if __name__ == "__main__":
    main()
