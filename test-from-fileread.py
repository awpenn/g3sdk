import gen3
from gen3 import submission
from gen3 import auth
import pandas
import json
from requests.auth import AuthBase
import requests
import hashlib

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
# dataset_consents = []
# with open('jsondumps/consents.json') as json_file:
#     data = json.load(json_file)["data"]
#     for consent in data:
#         dataset_consents.append(consent["key"])

#     for c in dataset_consents:
#         project_name = "NG00067"+"_"+c
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
        # with open("jsondumps/samples-from-dataset1.json", "r") as json_file:
        #     data = json.load(json_file)
        #     for key, sample in data.iteritems():
        #         ## some subjects have 'null' consent, ignoring for now
        #         if sample["subject"]["consent"] is not None:
        #             if sample["subject"]["consent"]["key"] == c:
        #                 sample_obj = {
        #                     "platform": sample["platform"], 
        #                     "type": "sample", 
        #                     "submitter_id": sample["key"], 
        #                     "molecular_datatype": sample["assay"], 
        #                     "sample_source": sample["source"], 
        #                     "subjects": {
        #                         "submitter_id": sample["subject"]["key"]
        #                     }
        #                 }
        #                 print(sample_obj)
        #                 submitter.submit_record("NG00067", project_name, sample_obj)

# ## loading phenotype data
# with open("jsondumps/ds1_phenotypes.json", "r") as json_file:
#     ## data here is a list created from all the phenotype nodes for a dataset
#     data = json.load(json_file)

#     for node in data:
#         if node["subject"]["key"] == 'C-ERF-90462':
#             print(node["phenotype"]["name"]+": "+node["value"])


## fileset builder
# urltail = 'datasets'
# request_url = APIURL+urltail+"/1/filesets"
# print('Getting fileset data from ' + request_url)
# response = requests.get(request_url, headers=headers)
# fileset_data = response.json()["data"]
# fetched_project_id = "1b7600d2-355b-5eb1-a2a5-c83399eb8906"

# dataset_consents = []
# with open('jsondumps/consents.json') as json_file:
#     data = json.load(json_file)["data"]
#     for consent in data:
#         dataset_consents.append(consent["key"])

# for c in dataset_consents:
#     for fileset in fileset_data:
#         print(fileset)
#         fileset_description = fileset["description"]
#         # # accession not yet in API data so will fake
#         # # fileset_name = fileset.accession + "_" + c
#         fileset_name = "fs0000"+str(fileset["id"])+"_"+c
#         # # accession not yet in API data so will fake
#         # # fileset_submitter_id = fileset.accession + "_" + c
#         fileset_submitter_id = "fs0000"+str(fileset["id"])+"_"+c

#         fileset_object =  {
#             "*projects": {
#             "id": fetched_project_id
#             }, 
#             "*description": fileset_description, 
#             "*fileset_name": fileset_name, 
#             "*type": "fileset", 
#             "*submitter_id": fileset_submitter_id
#         }
#         print(fileset_object)
file_md5 = hashlib.md5("wowowow").hexdigest()
print(file_md5)
## file manifests from query of filesets
fileset_sample_files_list = []
with open("jsondumps/fake-nonsample-fileset-manifest.json", "r") as json_file:
    ## data here is a list created from all the phenotype nodes for a dataset
    data = json.load(json_file)["data"]

    for file in data:
        fileset_sample_files_list.append(file)


    print(len(fileset_sample_files_list))
    print(fileset_sample_files_list[0])

with open("jsondumps/fake-nonsample-fileset-manifest.json", "r") as json_file:
    ## data here is a list created from all the phenotype nodes for a dataset
    data = json.load(json_file)["data"]

    for file in data:
        fileset_sample_files_list.append(file)


    print(len(fileset_sample_files_list))
    print(fileset_sample_files_list[0])
