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
# APIURL+urltail"/1/subjects 


response = requests.get(APIURL+urltail, headers=headers)
# response.json() produces a dictionary
data = response.json()
for dataset in data['data']:
    program_release_name = dataset["name"]
    program_dbgap = dataset["accession"]
    program_name = dataset["accession"]
    program_description = dataset["description"]
    dss_dataset_id = dataset["id"]
    dataset_consents = []
        
    program_obj = {
        "type": "program",
        "dbgap_accession_number": program_dbgap,
        "name": program_name,
        "release_name": program_release_name,
        "summary_description": program_description
    }
    # create programs from dataset list
    submitter.create_program(program_obj)
    # get guid for program based on program_name, store as fetched_id to link subjects, filesets, core_metadata_collections
    query = '{program(name:\"%s\"){id}}' % program_name

    fetched_program_id = submitter.query(query)["data"]["program"][0]["id"]

    # get a list of consents for the dataset
    print(dss_dataset_id)
    response = requests.get(APIURL+urltail+"/"+str(dss_dataset_id)+"/consents", headers=headers)
    data = response.json()["data"]
    for consent in data:
        dataset_consents.append(consent["key"])
    
    # create project for each consent in dataset_consents list
    for c in dataset_consents:
        project_obj = {
            "type": "project",
            "dbgap_accession_number": program_name+"_"+c,
            "name": program_name+"_"+c,
            "code": program_name+"_"+c,
            "availability_type": "Restricted"
        }

        submitter.create_project(program_name, project_obj)

        # create CMC for each created project

    

#for working with so dont call API a million times
# with open("datasets.json", "w") as outfile:
#     json.dump(response.json(), outfile)


# with open('datasets.json') as json_file:
#     data = json.load(json_file)
#     for dataset in data['data']:

#         project_release_name = dataset["name"]
#         project_dbgap = dataset["accession"]
#         project_name = dataset["accession"]
#         project_description = dataset["description"]
        
#         program_obj = {
#             "type": "program",
#             "dbgap_accession_number": project_dbgap,
#             "name": project_name,
#             "release_name": project_release_name,
#             "summary_description": project_description
#         }

#         sub.create_program(program_obj)



# sub.create_program(test)
# query = '{program(name:"test"){id}}'
# print(sub.query(query))
# delete = input("delete same record?")
# if delete == "y":
#   sub.delete_program("test")


