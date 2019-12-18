import gen3
from gen3 import submission
from gen3 import auth
import pandas as pd
import json
from requests.auth import AuthBase
import requests
import hashlib

## dataget/load functions in external file
from dataload_functions import *

from settings import TOKEN, APIURL, CTYPE, ACCEPT

headers = {
    'Content-Type': "application/json",
    'Accept': "application/json",
    "Authorization": "Bearer {token}".format(token=TOKEN),
    'User-Agent': "PostmanRuntime/7.18.0",
    'Cache-Control': "no-cache",
    'Postman-Token': "dd5ae43f-70d8-41e7-8911-55f1d3fa4d6d,0a7e9fcf-becc-49a0-9e86-7afa02e425d5",
    'Host': "dev3.niagads.org",
    'Accept-Encoding': "gzip, deflate",
    'Connection': "keep-alive",
    'cache-control': "no-cache"
}

Gen3Submission = submission.Gen3Submission
endpoint = "https://gen3test.lisanwanglab.org"
auth = auth.Gen3Auth(endpoint, refresh_file="credentials.json")

submitter = Gen3Submission(endpoint, auth)


print(test())