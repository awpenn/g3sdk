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
        project_name = "NG00067"+"_"+c
#     project_obj = {
#         "type": "project",
#         "dbgap_accession_number": program_name+"_"+c,
#         "name": program_name+"_"+c,
#         "code": program_name+"_"+c,
#         "availability_type": "Restricted",
#         "programs": [
#             {
#                 "id": fetched_program_id
#             }
#         ]
#     }

#     submitter.create_project(project_obj)


# # ## making CMCs
# dataset_consents = []
# # program_name = 'NG00067'

# with open('jsondumps/consents.json') as json_file:
#     data = json.load(json_file)["data"]
#     for consent in data:
#         dataset_consents.append(consent["key"])

# for c in dataset_consents:
#     project_name = "NG00067"+"_"+c
#     query = '{project(name:\"%s\"){id}}' % project_name
#     fetched_project_id = submitter.query(query)["data"]["project"][0]["id"]
#     print("fedged pid: " + fetched_project_id)
#     project_name = program_name+"_"+c
#     query = '{project(name:\"%s\"){id}}' % project_name
#     fetched_project_id = submitter.query(query)["data"]["project"][0]["id"]
#     print(fetched_project_id)

#     cmc_obj = {
#         "*collection_type": "Consent-Level File Manifest", 
#         "description": "Core Metadata Collection for "+program_name+"_"+c, 
#         "type": "core_metadata_collection", 
#         "submitter_id": program_name+"_"+c+"_"+"core_metadata_collection",
#         "projects": {
#             "id": fetched_project_id
#         }
#     }

#     print(cmc_obj)


## building sample/subject dictionary
#     project_sample_set = set({})

#     with open("jsondumps/samples-from-dataset1.json", "r") as json_file:
#         data = json.load(json_file)

#         for key, sample in data.iteritems():
#             print(sample["subject"]["consent"]["key"])
#             project_sample_set.add(sample["subject"]["key"])

#     submitter.submit_record(program_name, project_name, cmc_obj)

# ## building sample/subject dictionary
#     project_sample_set = set({})

#     with open("jsondumps/samples-from-dataset1.json", "r") as json_file:
#         data = json.load(json_file)
#         for key, sample in data.iteritems():
#             ## some subjects have 'null' consent, ignoring for now
#             if sample["subject"]["consent"] is not None:
#                 ## if the subject's consent matches the current project, add to the pss set
#                 if sample["subject"]["consent"]["key"] == c:
#                     project_sample_set.add( sample["subject"]["key"] )
        
#         if len(project_sample_set) > 0:
#             # print(c+ ": "+str(len(project_sample_set)))

#             for dictkey, value in enumerate(project_sample_set):
#                 for samplekey, sample in data.iteritems():
#                     if sample["subject"]["key"] == value:

#                         subject = sample["subject"]
#                         subject_obj = {
#                             "cohort": subject["cohort_key"], 
#                             "projects": {
#                                 "id": fetched_project_id
#                             }, 
#                             "consent": subject["consent"]["key"], 
#                             "type": "subject", 
#                             "submitter_id": subject["key"]
#                         }
#                         print(subject_obj)
#                         submitter.submit_record("NG00067", project_name, subject_obj)



# for key, value in enumerate(project_sample_set):
    # print(value)

## creating sample records
        with open("jsondumps/samples-from-dataset1.json", "r") as json_file:
            data = json.load(json_file)
            for key, sample in data.iteritems():
                ## some subjects have 'null' consent, ignoring for now
                if sample["subject"]["consent"] is not None:
                    if sample["subject"]["consent"]["key"] == c:
                        sample_obj = {
                            "platform": sample["platform"], 
                            "type": "sample", 
                            "submitter_id": sample["key"], 
                            "molecular_datatype": sample["assay"], 
                            "sample_source": sample["source"], 
                            "subjects": {
                                "submitter_id": sample["subject"]["key"]
                            }
                        }
                        print(sample_obj)
                        submitter.submit_record("NG00067", project_name, sample_obj)