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

# write an api response to a .json file and then test off that so not hitting the API over and over

response = requests.get(APIURL+urltail+"/1/consents.key", headers=headers)
# response.json() produces a dictionary
print(response.json())
# query = "{program(name:\"NG00067\"){id}}"

# response = submitter.query(query)

#set dump file name
dumpfile_name = ''

with open("jsondumps/%s.json" % dumpfile_name, "a") as outfile:
    # below, data from DSS api requires response.json() , from datastage = response
    json.dump(response.json(), outfile)
