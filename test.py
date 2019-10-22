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
        
    program_obj = {
        "type": "program",
        "dbgap_accession_number": program_dbgap,
        "name": program_name,
        "release_name": program_release_name,
        "summary_description": program_description
    }
    # create programs from dataset list
    print( "creating program node for " + program_dbgap )
    submitter.create_program(program_obj)
    # get guid for program based on program_name, store as fetched_id to link subjects, filesets, core_metadata_collections
    query = '{program(name:\"%s\"){id}}' % program_name

    fetched_program_id = submitter.query(query)["data"]["program"][0]["id"]


    ## get subjects/samples by querying sampleSets of a dataset with dss_dataset_id
    urltail = 'datasets'
    print( APIURL+urltail+"/"+str(dss_dataset_id)+"/sampleSets" )
    response = requests.get(APIURL+urltail+"/"+str(dss_dataset_id)+"/sampleSets", headers=headers)
    ## a list of sample sets for a dataset
    sample_set_data = response.json()["data"]
    
    sample_dict = {}

    for sample_set in sample_set_data:    
        urltail = 'sampleSets'
        print( APIURL+urltail+"/"+str(sample_set["id"])+"/samples?includes=subject.fullConsent" )
        response = requests.get(APIURL+urltail+"/"+str(sample_set["id"])+"/samples?includes=subject.fullConsent", headers=headers)

        ## merges multiple dictionaries into one list of dictionaries
        for key, dictionary in response.json().iteritems():
            for sample in dictionary:
                sample_dict[sample["key"]] = sample
             
    ## sample dict after this for loop will be all the samples for a dataset (some subjects have multi samples 4870 subj/4909 sample in dev server)
    ## sample_set is set of unique subject_ids in the sample_dict

    
    ## get a list of consents for the dataset
    dataset_consents = []
    urltail = 'datasets'
    response = requests.get(APIURL+urltail+"/"+str(dss_dataset_id)+"/consents", headers=headers)

    consent_data = response.json()["data"]
    for consent in consent_data:
        dataset_consents.append(consent["key"])

    ## create a set of unique subject ids for subjects whose consent matches the current consent-project being created
    for c in dataset_consents:
        project_sample_set = set({})
        for key, sample in sample_dict.iteritems():
            ## some subjects have 'null' consent, ignoring for now
            if sample["subject"]["consent"] is not None:
                ## if the subject's consent matches the current project, add to the pss set
                if sample["subject"]["consent"]["key"] == c:
                    project_sample_set.add( sample["subject"]["key"] )

        ## create project for each consent in dataset_consents list, if it has any associated subjects
        if len(project_sample_set) > 0:
            project_obj = {
                "type": "project",

                "dbgap_accession_number": program_name+"_"+c,
                "name": program_name+"_"+c,
                "code": program_name+"_"+c,
                "availability_type": "Restricted"
            }
            print( "Creating project node for " + program_name+"_"+ c )
            submitter.create_project(program_name, project_obj)

            ## create CMC for each created project

            project_name = program_name+"_"+c
            query = '{project(name:\"%s\"){id}}' % project_name
            fetched_project_id = submitter.query(query)["data"]["project"][0]["id"]
            print( fetched_project_id )

            cmc_obj = {
                "*collection_type": "Consent-Level File Manifest", 
                "description": "Core Metadata Collection for "+program_name+"_"+c, 
                "type": "core_metadata_collection", 
                "submitter_id": program_name+"_"+c+"_"+"core_metadata_collection",
                "projects": {
                    "id": fetched_project_id
                }
            }
            print( "creating core_metadata_collection for " + program_name+"_" + c )
            submitter.submit_record(program_name, project_name, cmc_obj)

            ## for subject in dictionary, create a subject record
            print( "there will be %s subjects in this project" % len(project_sample_set) )

            for dictkey, value in enumerate(project_sample_set):
                ## AW - why doesn't this create two subjects for subjects with multiple samples...but if tied to same subject entity in db then maybe overwrites with same info?
                for samplekey, sample in sample_dict.iteritems():
                    if sample["subject"]["key"] == value:

                        subject = sample["subject"]
                        subject_obj = {
                            "cohort": subject["cohort_key"], 
                            "projects": {
                                "id": fetched_project_id
                            }, 
                            "consent": subject["consent"]["key"], 
                            "type": "subject", 
                            "submitter_id": subject["key"]
                        }
                        print( "creating subject record for " + subject["key"] )
                        submitter.submit_record(program_name, project_name, subject_obj)

                        ## create a sample node for each passing through here while we're at it
                        print( "creating sample record(s) for " + sample["key"] )
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
                        submitter.submit_record(program_name, project_name, sample_obj)

                        ## do phenotypes next? (see slack for how to get values)

