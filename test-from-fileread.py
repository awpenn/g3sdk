import gen3
from gen3 import submission
from gen3 import auth
import pandas
import json
from requests.auth import AuthBase
import requests

from settings import TOKEN, APIURL, CTYPE, ACCEPT

urltail = "datasets"
headers = {
    'Content-Type': "application/json",
    'Accept': "application/json",
    "Authorization": "Bearer {token}".format(token=TOKEN),
    'User-Agent': "PostmanRuntime/7.18.0",
    'Cache-Control': "no-cache",
    'Postman-Token': "dd5ae43f-70d8-41e7-8911-55f1d3fa4d6d,0a7e9fcf-becc-49a0-9e86-7afa02e425d5",
    'Host': "dev2.niagads.org",
    'Accept-Encoding': "gzip, deflate",
    'Connection': "keep-alive",
    'cache-control': "no-cache"
}

Gen3Submission = submission.Gen3Submission
endpoint = "https://gen3test.lisanwanglab.org"
auth = auth.Gen3Auth(endpoint, refresh_file="credentials.json")

submitter = Gen3Submission(endpoint, auth)
# how to add to base string
# APIURL+"/1/subjects 


# response = requests.get(APIURL+urltail, headers=headers)
# # response.json() produces a dictionary
# for working with so dont call API a million times
# with open("datasets.json", "w") as outfile:
#     json.dump(response.json(), outfile)

## unpacking program_ids
# with open('jsondumps/fetched_program_id.json') as json_file:
#    data = json.load(json_file)
#    print(data["data"]["program"][0]["id"])

## getting consents, as list?
dataset_consents = []
with open('jsondumps/consents.json') as json_file:
    data = json.load(json_file)["data"]
    for consent in data:
        dataset_consents.append(consent["key"])

for c in dataset_consents:
    project_obj = {
        "type": "project",
        "dbgap_accession_number": program_name+"_"+c,
        "name": program_name+"_"+c,
        "code": program_name+"_"+c,
        "availability_type": "Restricted",
        "programs": [
            {
                "id": fetched_program_id
            }
        ]
    }

    submitter.create_project(project_obj)


