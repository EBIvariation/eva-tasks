import time

import globus_sdk
import json
import os

from globus_sdk import TransferAPIError

TOKEN_FILE = os.path.expanduser("~/.eva-sub-cli/globus_tokens.json")
TRANSFER_SCOPE = "urn:globus:auth:scope:transfer.api.globus.org:all"
OFFLINE_SCOPE = "offline_access"

CLIENT_ID = ""
DEST_ENDPOINT_UUID = ""
DEST_ENDPOINT_PATH = ''
SOURCE_ENDPOINT_UUID = ""
SOURCE_PATH = ''
SOURCE_DATA_ACCESS = f"urn:globus:auth:scope:{SOURCE_ENDPOINT_UUID}:data_access"

def save_tokens(tokens):
    with open(TOKEN_FILE, "w") as open_file:
        json.dump(tokens, open_file)


def load_tokens():
    if not os.path.exists(TOKEN_FILE):
        return None
    with open(TOKEN_FILE, "r") as open_file:
        return json.load(open_file)


def do_login_flow():
    client = globus_sdk.NativeAppAuthClient(CLIENT_ID)
    client.oauth2_start_flow(
        requested_scopes=f"{TRANSFER_SCOPE} {OFFLINE_SCOPE}"
    )
    print("Go to this URL and login:", client.oauth2_get_authorize_url())
    auth_code = input("Enter the auth code here: ").strip()

    token_response = client.oauth2_exchange_code_for_tokens(auth_code)
    save_tokens(token_response.by_resource_server)
    return token_response.by_resource_server


def get_authorizer():
    tokens = load_tokens()
    client = globus_sdk.NativeAppAuthClient(CLIENT_ID)

    if not tokens:
        tokens = do_login_flow()

    # Get the transfer tokens
    transfer_tokens = tokens["transfer.api.globus.org"]

    # Use RefreshTokenAuthorizer so tokens auto-refresh
    authorizer = globus_sdk.RefreshTokenAuthorizer(
        transfer_tokens["refresh_token"],
        client,
        access_token=transfer_tokens["access_token"],
        expires_at=transfer_tokens["expires_at_seconds"],
        on_refresh=save_tokens,  # save updated tokens automatically
    )
    return authorizer


def does_path_exists(tc, endpoint_id, file_path):
    try:
        # Try listing the parent directory and see if the entry is there
        parent_path = os.path.dirname(file_path)
        file_name = os.path.basename(file_path)

        for entry in tc.operation_ls(endpoint_id, path=parent_path)["DATA"]:
            if entry["name"] == file_name:
                return True, entry["type"]  # "file" or "dir"
        return False, None

    except TransferAPIError as e:
        if e.http_status == 404:
            return False, None
        raise


def wait_for_completion(tc, task_id, interval=10, timeout=600):
    total_time = 0
    while True:
        task = tc.get_task(task_id)
        status = task["status"]
        print(f"Task {task_id}: {status}")

        if status in ("SUCCEEDED", "FAILED", "INACTIVE"):
            return status

        time.sleep(interval)
        total_time += interval
        if total_time >= timeout:
            raise TimeoutError(f"Task {task_id} status: {status} timed out")


def transfer(tc, source_endpoint, source_path):
    # Create the transfer task
    transfer_data = globus_sdk.TransferData(
        tc,
        source_endpoint,
        DEST_ENDPOINT_UUID,
        label="Automated Service Transfer",
        sync_level="checksum"  # ensures integrity
    )

    path_exists, path_type = does_path_exists(tc, source_endpoint, source_path)
    if path_exists:
        dest_path = os.path.join(DEST_ENDPOINT_PATH, os.path.basename(source_path))
        transfer_data.add_item(source_path, dest_path)

    # Submit the transfer
    transfer_result = tc.submit_transfer(transfer_data)
    print("Submitted Transfer, Task ID:", transfer_result["task_id"])
    status = wait_for_completion(tc, transfer_result["task_id"])
    print("Final status:", status)

def main():

    tc = globus_sdk.TransferClient(authorizer=get_authorizer())
    # Example: List your tasks
    for task in tc.task_list():
        print(task["task_id"], task["status"])

    # This is to find endpoint that the user has access to
    source_endpoint = None
    for ep in tc.endpoint_search(filter_scope="my-endpoints"):
        source_endpoint = ep["id"]
        print(f'Will use {ep["id"]}, {ep["display_name"]}')

    transfer(tc, SOURCE_ENDPOINT_UUID, SOURCE_PATH)

if __name__ == '__main__':
    main()