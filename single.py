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


urltail = 'datasets'
request_url = APIURL+urltail+"/"+str(1)+"/subjectPhenotypes?includes=phenotype,subject&per_page=11000"
response = requests.get(request_url, headers=headers)
last_page = response.json()["meta"]["last_page"]
phenotype_data = response.json()["data"]

    ## this list is all the phenotype nodes from all the pages
print( "creating phenotype list for dataset " + str(1) )
print( "api returning %s page(s) of phenotypes" % str(last_page) ) 
project_phenotype_list = []

for phenotype in phenotype_data:
    project_phenotype_list.append(phenotype)

if last_page > 1:
    for page in range( last_page + 1 ):
        if page < 2:
            continue
        else:
            response = requests.get(request_url+"&page="+str(page), headers=headers)
            phenotype_data = response.json()["data"]
            for phenotype in phenotype_data:
                project_phenotype_list.append(phenotype)


dropped_pheno_subjects = []

with open("jsondumps/subjects-pheno-dropped.json", "r") as json_file:
    data = json.load(json_file)

    for subject in data:
        dropped_pheno_subjects.append(subject["submitter_id"])

    for subject in dropped_pheno_subjects:
        current_subject_phenotypes_dict = {}

        for pnode in project_phenotype_list:
            if pnode["subject"]["key"] == subject:
                                        # print(pnode["phenotype"]["name"]+": "+pnode["value"]) = phenotype: phenotype value

                if pnode["phenotype"]["name"] == 'apoe':
                    current_subject_phenotypes_dict["apoe"] = pnode["value"]

                if pnode["phenotype"]["name"] == 'sex':
                    current_subject_phenotypes_dict["sex"] = pnode["value"]

                if pnode["phenotype"]["name"] == 'race':
                    current_subject_phenotypes_dict["race"] = pnode["value"]

                if pnode["phenotype"]["name"] == 'ethnicity':
                    current_subject_phenotypes_dict["ethnicity"] = pnode["value"]

                if pnode["phenotype"]["name"] == 'dx':
                    current_subject_phenotypes_dict["dx"] = pnode["value"]

        phenotype_obj = {
            "APOE": current_subject_phenotypes_dict["apoe"], 
            "sex": current_subject_phenotypes_dict["sex"], 
            "subjects": {
                "submitter_id": subject
            }, 
            "race": current_subject_phenotypes_dict["race"], 
            "type": "phenotype", 
            "diagnosis": current_subject_phenotypes_dict["dx"], 
            "submitter_id": subject + "_pheno", 
            "ethnicity": current_subject_phenotypes_dict["ethnicity"]
        }

        print("record for " + subject)
        print(phenotype_obj)
        # submitter.submit_record("NG00067", "NG00067_DS-DEMND-IRB-PUB-NPU", phenotype_obj)