import gen3
from gen3 import submission
from gen3 import auth
import pandas
import json
from requests.auth import AuthBase
import requests

from settings import TOKEN, APIURL, CTYPE, ACCEPT

headers = {"Authorization": "Bearer {token}".format(token=TOKEN)}
# testing requests on gnews open API
# r = requests.get('https://gnews.io/api/v3/search?q=wisconsin&token={key}'.format(key=TOKEN))
# returned = r.json()
# for i in returned["articles"]:
#     print(i["description"])
# Gen3Submission = submission.Gen3Submission
# endpoint = "https://gen3test.lisanwanglab.org"
# auth = auth.Gen3Auth(endpoint, refresh_file="credentials.json")

# sub = Gen3Submission(endpoint, auth)

# test = {
#   "type": "program",
#   "dbgap_accession_number": "test",
#   "name": "test",
#   "release_name": "test"
# }

# sub.create_program(test)
# query = '{program(name:"test"){id}}'
# print(sub.query(query))
# delete = input("delete same record?")
# if delete == "y":
#   sub.delete_program("test")


