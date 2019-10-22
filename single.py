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
## this worked for submitting a subject record
# subject = {
#   "cohort": "RAS", 
#   "*projects": {
#     "id": "de6a8e65-c574-5087-aa7a-6cc68d57310b"
#   }, 
#   "*consent": "all", 
#   "*type": "subject", 
#   "*submitter_id": "A-RAS-0900999"
# }

# submitter.submit_record("NG00067", "NG00067_DS-ADRD-IRB-PUB-NPU", subject)

# cmc_obj = {
#         "collection_type": "Consent-Level File Manifest", 
#         "description": "Core Metadata Collection", 
#         "type": "core_metadata_collection", 
#         "submitter_id": "wowowoww",
#         "projects": {
#             "id": "de6a8e65-c574-5087-aa7a-6cc68d57310b"
#         }
#     }   

# submitter.submit_record("NG00067", "NG00067_DS-ADRD-IRB-PUB-NPU", cmc_obj)
