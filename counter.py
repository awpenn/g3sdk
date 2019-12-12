import gen3
from gen3 import submission
from gen3 import auth
import pandas as pd
import json
from requests.auth import AuthBase
import requests
import hashlib

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

searchy = ["DS-ND-IRB-PUB-MDS"]
Gen3Submission = submission.Gen3Submission
endpoint = "https://gen3test.lisanwanglab.org"
auth = auth.Gen3Auth(endpoint, refresh_file="credentials.json")

submitter = Gen3Submission(endpoint, auth)

def count_by_consent(consent):
    count = 0
    consent = consent
    response = requests.get(APIURL+"datasetVersions/2/sampleSets", headers=headers)
        ## a list of sample sets for a dataset
    sample_set_data = response.json()["data"]
        
    sample_dict = []
    for sample_set in sample_set_data:            

        request_url = APIURL+"sampleSets/"+str(sample_set["id"])+"/samples"
        print('checking to see if there are samples from files from ' + request_url)
        response = requests.get(request_url, headers=headers)

        if len(response.json()["data"]) > 0:
            request_url = APIURL+"sampleSets/"+str(sample_set["id"])+"/samples?includes=subject.fullConsent&per_page=1000"
            print( 'getting samples from ' + request_url )
            response = requests.get(request_url, headers=headers)  

            last_page = response.json()["meta"]["last_page"]
            sample_set_subject_data = response.json()["data"]

            for sample in sample_set_subject_data:
                sample_dict.append(sample)
            if last_page > 1:
                for page in range( last_page + 1 ):
                    if page < 2:
                        continue
                    else:
                        print("page of samples: got " + str(page))
                        response = requests.get(request_url + "&page=" + str(page), headers=headers)
                        print('Retrieving samples from ' + request_url + "&page=" + str(page))
                        sample_set_subject_data = response.json()["data"]

                        for sample in sample_set_subject_data:
                            sample_dict.append(sample)
            else:
                continue
                                
        else:
            print('no samples in this sampleSet. Moving on...')
            continue

    for sample in sample_dict:
        if sample["subject"]["consent_key"].strip() == consent:
            count += 1

    print('count for ' + consent + " = " + str(count))

for i in searchy:
    count_by_consent(i)


def count_by_dataset():
    count = 0

    response = requests.get(APIURL+"datasetVersions/2/sampleSets", headers=headers)
        ## a list of sample sets for a dataset
    sample_set_data = response.json()["data"]
        
    sample_dict = []
    for sample_set in sample_set_data:            

        request_url = APIURL+"sampleSets/"+str(sample_set["id"])+"/samples"
        print('checking to see if there are samples from files from ' + request_url)
        response = requests.get(request_url, headers=headers)
        if len(response.json()["data"]) > 0:
            request_url = APIURL+"sampleSets/"+str(sample_set["id"])+"/samples?includes=subject.fullConsent&per_page=1000"
            print( 'getting samples from ' + request_url )
            response = requests.get(request_url, headers=headers)  

            last_page = response.json()["meta"]["last_page"]
            sample_set_subject_data = response.json()["data"]

            for sample in sample_set_subject_data:
                sample_dict.append(sample)
            if last_page > 1:
                for page in range( last_page + 1 ):
                    if page < 2:
                        continue
                    else:
                        print("page of samples: got " + str(page))
                        response = requests.get(request_url + "&page=" + str(page), headers=headers)
                        print('Retrieving samples from ' + request_url + "&page=" + str(page))
                        sample_set_subject_data = response.json()["data"]

                        for sample in sample_set_subject_data:
                            sample_dict.append(sample)
                                
            else:
                print('no samples in this sampleSet. Moving on...')
                continue
    
    print(len(sample_dict))

def count_subjects_from_samples():
    count = 0
    response = requests.get(APIURL+"datasetVersions/2/sampleSets", headers=headers)
        ## a list of sample sets for a dataset
    sample_set_data = response.json()["data"]
        
    uniq_subject_id_set = set({})
    overall_subject_id_list = []
    for sample_set in sample_set_data:            

        request_url = APIURL+"sampleSets/"+str(sample_set["id"])+"/samples"
        print('checking to see if there are samples from files from ' + request_url)
        response = requests.get(request_url, headers=headers)
        if len(response.json()["data"]) > 0:
            request_url = APIURL+"sampleSets/"+str(sample_set["id"])+"/samples?includes=subject.fullConsent&per_page=1000"
            print( 'getting samples from ' + request_url )
            response = requests.get(request_url, headers=headers)  

            last_page = response.json()["meta"]["last_page"]
            sample_set_subject_data = response.json()["data"]

            for sample in sample_set_subject_data:
                uniq_subject_id_set.add( sample["subject"]["key"] )
                overall_subject_id_list.append( sample["subject"]["key"] )         
            if last_page > 1:
                for page in range( last_page + 1 ):
                    if page < 2:
                        continue
                    else:
                        print("page of samples: got " + str(page))
                        response = requests.get(request_url + "&page=" + str(page), headers=headers)
                        print('Retrieving samples from ' + request_url + "&page=" + str(page))
                        sample_set_subject_data = response.json()["data"]

                        for sample in sample_set_subject_data:
                            uniq_subject_id_set.add( sample["subject"]["key"] )
                            overall_subject_id_list.append( sample["subject"]["key"] )                                
            else:
                print('no samples in this sampleSet. Moving on...')
                continue
    
    print("unique subject ids returned " + str(len(uniq_subject_id_set)))
    print("total ids returned " + str(len(overall_subject_id_list)))

