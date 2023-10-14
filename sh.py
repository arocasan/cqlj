import base64
import json
import logging.config
import multiprocessing as mp
import os
import re
import sys
import time
import urllib.parse
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv

today = datetime.today().strftime("%Y-%m-%d-%H-%M-%S")


def aroca_logger():
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    logging.basicConfig(
        handlers=[
            logging.FileHandler(f"logs/{today}_assets_server_cloud.log"),
            console_handler,
        ],
        format="[ %(asctime)s ]-[ %(process)d ]-[ %(levelname)s ]-[ %(message)s ]",
        level=logging.DEBUG,
    )


aroca_logger()

# Colors
BLUE = "\33[34m"
LBLUE = "\33[94my"
GREEN = "\33[92m"
RED = "\33[91m"
YELLOW = "\33[93m"
PURP = "\33[95m"
BOLD = "\33[1m"
END = "\33[0m"

load_dotenv()

server_pat = os.getenv("JIRA_PAT")
server_url = os.getenv("JIRA_URL")
asset_url = os.getenv("ASSET_ENDPOINT")
user_email = os.getenv("CLOUD_EMAIL")
api_token = os.getenv("CLOUD_TOKEN")
site_url = os.getenv("CLOUD_URL")


server = requests.Session()
cloud = requests.Session()


server.headers = {
    "Authorization": f"Bearer {server_pat}",
    "Content-Type": "application/json",
}
auth_str = f"{user_email}:{api_token}"
encoded_auth_str = base64.b64encode(auth_str.encode()).decode()

cloud.headers = {
    "Authorization": f"Basic {encoded_auth_str}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

get_user = cloud.get(f"{site_url}/rest/api/3/myself")

if get_user.status_code != 200:
    logging.error(f"Error: {get_user.status_code} - {get_user.text}")
    sys.exit(1)
else:
    logging.info(f"Site: {site_url} | User: {get_user.json()['displayName']}")

# Get all assets from server

logging.info("Getting all assets from server")


def get_customfield_id(session, url, asset_field=None):
    asset_field = None
    # project=SCRIPTLOCA and "Affected Server" is not emptytarget_custom_field = input("Enter the name of the target custom field: ")
    get_asset_field = session.get(f"{url}/rest/api/2/field")
    get_asset_field = get_asset_field.json()

    asset_field_id = None

    while asset_field_id is None:
        asset_field = input(
            f"Enter the name of the {BLUE}{BOLD}[Source] Asset field:{END}\n"
        )
        for field in get_asset_field:
            if field.get("name") == f"{asset_field}":
                asset_field_id = field["id"]
                logging.info(f"Asset field id found: {asset_field_id}")
                break

        if asset_field_id is None:
            logging.info(f"{asset_field} field not found")

    return asset_field_id, asset_field


def get_issues(server, server_url, asset_field_id, jql):
    all_issues = []
    start_at = 0
    max_results = 1000
    total_issues = 1
    updated_issues = 0
    start_time = time.time()
    while start_at < total_issues:
        issues = server.get(
            f"{server_url}/rest/api/2/search?jql={jql}&fields={asset_field_id}&maxResults={max_results}&startAt={start_at}"
        )

        start_at += max_results

        # if issues.status_code == 200:
        issues = issues.json()
        total_issue_count = issues["total"]
        total_issues = total_issue_count

        logging.info(f"Found {total_issue_count} issues")

        for issue in issues["issues"]:
            issue_key = issue["key"]
            issue_id = issue["id"]
            logging.info(f"Processing issue: {issue_key}")
            asset_field = issue["fields"][asset_field_id]

            string = asset_field[0]
            match = re.search(r"\((\w+-\d+)\)", string)
            if match:
                origin_asset_object_key = match.group(1)
                updated_issues += 1

                all_issues.append(
                    [
                        issue_key,
                        origin_asset_object_key,
                        issue_id,
                    ]
                )

                logging.info(
                    f"Found Origin asset object key: {origin_asset_object_key} for Issue {issue_key}"
                )
            percentage = int(updated_issues / total_issue_count * 100)
            elapsed_time = time.time() - start_time
            elapsed_time_hours, elapsed_time_remainder = divmod(elapsed_time, 3600)
            elapsed_time_minutes, elapsed_time_seconds = divmod(
                elapsed_time_remainder, 60
            )

            logging.info(
                f"Procced {updated_issues} issues of {total_issue_count} [{percentage}%] | Elapsed time: {elapsed_time_hours:.0f}h {elapsed_time_minutes:.0f}m {elapsed_time_seconds:.0f}s"
            )
            logging.info(f"-" * 50)

    return all_issues


def export_issues(all_issues, asset_field, asset_field_id, today):
    filename = f"exports/{asset_field}_{asset_field_id}_{today}.csv"
    df = pd.DataFrame(
        all_issues,
        columns=["issue_key", "origin_asset_object_key", "origin_issue_id"],
    ).to_csv(filename, index=False)
    logging.info(f"Exported {len(all_issues)} issues to {filename}")
    return filename


def get_cloud_issues(cloud, site_url, path):
    cloud_issues = pd.read_csv(path)
    start_time = time.time()
    updated_issues = 0
    total_issue_count = len(cloud_issues)
    (field_to_update, asset_field) = get_customfield_id(cloud, site_url)
    return (
        cloud_issues,
        start_time,
        updated_issues,
        total_issue_count,
        field_to_update,
        asset_field,
    )


def process_issue(
    server_issue,
    cloud,
    asset_url,
    cloud_issues,
    start_time,
    updated_issues,
    total_issue_count,
    field_to_update,
    asset_field,
):
    logging.warning("*" * 50)
    logging.warning(server_issue)
    logging.warning("*" * 50)
    server_key = server_issue[0]
    origin_asset_object_key = server_issue[1]

    logging.info(f"Processing issue: {server_key}")
    logging.info(f"Origin asset object key: {origin_asset_object_key}")

    payload = json.dumps(
        {
            "qlQuery": f'"Original Object Key" = {origin_asset_object_key}',
        }
    )

    cloud_object_key = cloud.post(asset_url, data=payload)

    if cloud_object_key.status_code == 429:
        logging.info(
            f"Rate limit warning: {cloud_object_key.status_code} - {cloud_object_key.text}  - Waiting 60 seconds"
        )
        time.sleep(60)
        cloud_object_key = cloud.post(asset_url, data=payload)

    if cloud_object_key.status_code == 200:
        cloud_objects = cloud_object_key.json()
        print(cloud_object_key)
        cloud_object_key = cloud_objects["values"][0]["objectKey"]
        print(cloud_object_key)
        cloud_global_object_id = cloud_objects["values"][0]["globalId"]
        print(cloud_global_object_id)
        logging.info(
            f"Found Cloud asset object key: {cloud_object_key}; [{cloud_global_object_id}]"
        )

        cloud_issues.append(
            [
                server_key,
                origin_asset_object_key,
                cloud_object_key,
                cloud_global_object_id,
            ]
        )
        updated_issues += 1

    else:
        logging.info(f"Error: {cloud_object_key.status_code} - {cloud_object_key.text}")
        sys.exit(1)

    percentage = int(updated_issues / total_issue_count * 100)
    elapsed_time = time.time() - start_time
    elapsed_time_hours, elapsed_time_remainder = divmod(elapsed_time, 3600)
    elapsed_time_minutes, elapsed_time_seconds = divmod(elapsed_time_remainder, 60)

    logging.info(
        f"Procced {updated_issues} issues of {total_issue_count} [{percentage}%] | Elapsed time: {elapsed_time_hours:.0f}h {elapsed_time_minutes:.0f}m {elapsed_time_seconds:.0f}s"
    )
    logging.info(f"-" * 50)

    return cloud_issues, updated_issues


def import_issues(
    cloud_issues,
    site_url,
    start_time,
    updated_issues,
    total_issue_count,
    field_to_update,
):
    for cloud_issue in cloud_issues.to_dict("records"):
        cloud_key = cloud_issue["server_key"]
        cloud_object_key = cloud_issue["cloud_asset_object_key"]
        cloud_global_object_id = cloud_issue["cloud_asset_global_object_id"]

        logging.info(f"Processing issue: {cloud_key}")
        logging.info(f"Cloud asset object key: {cloud_object_key}")
        logging.info(f"Cloud asset object ID: {cloud_global_object_id}")

        payload = json.dumps(
            {"fields": {f"{field_to_update}": [{"id": f"{cloud_global_object_id}"}]}}
        )

        edit_issue = cloud.put(
            f"{site_url}/rest/api/3/issue/{cloud_key}",
            data=payload,
        )

        if edit_issue.status_code == 429:
            logging.warning(
                f"Rate limit warning: {edit_issue.status_code} - {edit_issue.text}  - Waiting 60 seconds"
            )
            time.sleep(60)
            edit_issue = cloud.put(
                f"{site_url}/rest/api/3/issue/{cloud_key}",
                data=payload,
            )

        if edit_issue.status_code == 204:
            logging.info(f"Updated issue: {cloud_key}")
            updated_issues += 1
        else:
            logging.info(f"Error: {edit_issue.status_code} - {edit_issue.text}")

        percentage = int(updated_issues / total_issue_count * 100)
        elapsed_time = time.time() - start_time
        elapsed_time_hours, elapsed_time_remainder = divmod(elapsed_time, 3600)
        elapsed_time_minutes, elapsed_time_seconds = divmod(elapsed_time_remainder, 60)

        logging.info(
            f"Procced {updated_issues} issues of {total_issue_count} [{percentage}%] | Elapsed time: {elapsed_time_hours:.0f}h {elapsed_time_minutes:.0f}m {elapsed_time_seconds:.0f}s"
        )
        logging.info(f"-" * 50)


def main():
    (asset_field_id, asset_field) = get_customfield_id(server, server_url)
    jql = input(
        f'Enter JQL. Eg:\n{BLUE}{BOLD}project=SCRIPTLOCA and "Affected Server" is not empty{END}:\n'
    )
    jql = urllib.parse.quote(jql)

    logging.info(f"Running JQL: {jql}")

    all_issues = get_issues(server, server_url, asset_field_id, jql)

    filename = export_issues(all_issues, asset_field, asset_field_id, today)

    path = input(
        f"Enter the path to the file to map keys to cloud. Eg:\n{BLUE}{BOLD}exports/{asset_field}_{asset_field_id}_{today}.csv{END}:\n"
    )

    (
        cloud_issues,
        start_time,
        updated_issues,
        total_issue_count,
        field_to_update,
        asset_field,
    ) = get_cloud_issues(cloud, site_url, path)

    with mp.Pool() as pool:
        results = [
            pool.apply_async(
                process_issue,
                args=(
                    server_issue,
                    cloud,
                    asset_url,
                    cloud_issues,
                    start_time,
                    updated_issues,
                    total_issue_count,
                    field_to_update,
                    asset_field,
                ),
            )
            for server_issue in all_issues
        ]
        for result in results:
            cloud_issues, updated_issues = result.get()

    filename = f"exports/{asset_field}_{asset_field_id}_{today}_cloud.csv"
    df2 = pd.DataFrame(
        cloud_issues,
        columns=[
            "server_key",
            "origin_asset_object_key",
            "cloud_asset_object_key",
            "cloud_asset_global_object_id",
        ],
    ).to_csv(filename, index=False)
    logging.info(f"Exported {len(cloud_issues)} issues to {filename}")

    path = input(
        f"Enter the path to the file to import to cloud. Eg:\n{BLUE}{BOLD}exports/{asset_field}_{asset_field_id}_{today}_cloud.csv{END}:\n"
    )

    cloud_issues = pd.read_csv(path)

    import_issues(
        cloud_issues,
        site_url,
        start_time,
        updated_issues,
        total_issue_count,
        field_to_update,
    )


if __name__ == "__main__":
    main()
