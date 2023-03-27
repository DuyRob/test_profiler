import requests
import os

# Retrieve value from environment variable
api_token = os.environ.get("GITLAB_CI_DBTCLOUD_API_TOKEN")
account_id = os.environ.get("GITLAB_CI_DBTCLOUD_ACCOUNT_ID")
job_id = os.environ.get("GITLAB_CI_DBTCLOUD_PROD_JOB_ID")


# Set the headers
headers = {
    "Content-Type": "application/json",
    "Authorization": "Token " + api_token
}

# Set the URL
url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/?limit=1&job_definition_id={job_id}&order_by=-id&status=10"

# Make the API call
response = requests.get(url, headers=headers)
latest_run_id = response.json()["data"][0]['id']

url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{latest_run_id}/artifacts/manifest.json"
response = requests.get(url, headers=headers)

with open("manifest.json", "w") as f:
    # Write the response content to the file
    f.write(response.text)


