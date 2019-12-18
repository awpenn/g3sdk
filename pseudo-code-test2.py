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

Gen3Submission = submission.Gen3Submission
endpoint = "https://gen3test.lisanwanglab.org"
auth = auth.Gen3Auth(endpoint, refresh_file="credentials.json")

submitter = Gen3Submission(endpoint, auth)


## Initial call to api, get list of datasets, starts `for dataset in datasets` loop.

## Creates program object and then calls gatherData(dss_dataset_id), which has subfunctions:
## getFilesAndPhenotypes(dss_dataset_id), getSamplesSubjects(dss_dataset_id), getConsents(dss_dataset_id), !which all return there collected data! 
## and the gatherData returns an array of these arrays

## call to createProject(*args) 



for dataset in datasets:
    ## dss_dataset_id = dataset["id"]
    program_name = dataset["accession"]
    program_url = build_dataset_url( program_name )
    
    for version in dataset["datasetVersions"]:
        if version["active"] == 1:

            ## for now (12/10) the 'active version in dss doesnt have the harmonized phenos, so for the whole deal, gonna hardcode datasetVersion2
            dss_dataset_id = version["id"]
    ## AW- production will have latest_release as a variable
    program_obj = {
        "type": "program",
        "dbgap_accession_number": dataset["accession"],
        "name": dataset["accession"],
        "release_name": dataset["name"],
        "summary_description": dataset["description"],
        "dataset_url": program_url, 
    }

    ## create programs from dataset list
    print( "creating program node for " + dataset["accession"] )

    submitter.create_program(program_obj)

    filesAndPhenotypes = getFilesPhenotypes(dss_dataset_id) ##array of arrays, fileSamp, nonSamp, allCon, phenotypes
    samplesAndSubjects = getSamplesSubjects(dss_dataset_id) ## sampleDict (with subject info `included`)
    consents = getConsents(dss_dataset_id)

    for consent in consents:
        createProject(consent, program_name, filesAndPhenotypes, samplesAndSubjects)
    
    #...#
    def getFilesPhenotypes(dss_dataset_id):
        fileSamples = getSampleFiles(dss_dataset_id)
        fileNonSamples = getNonSampleFiles(dss_dataset_id)
        fileAllCon = getAllConFiles(dss_dataset_id)
        phenotypes = getPhenotypes(dss_dataset_id)

        return [fileSamples, fileNonSamples, fileAllCon, phenotypes]
    
    def getSamplesSubjects(dss_dataset_id):
        sample_dict = {}

        request.yadaydayda......

        return sample_dict
    
    def createProject(consent, program_name, filesAndPhenotypes, samplesAndSubjects):
        project_sample_set = set({})
        for key, sample in sample_dict.iteritems():
            ## some subjects have 'null' consent, ignoring for now
            if sample["subject"]["consent"] is not None:
                ## if the subject's consent matches the current project, add to the pss set
                if sample["subject"]["consent"]["key"].strip() == c:
                    project_sample_set.add( sample["subject"]["key"] )
        
        ## trying to debug why its not finding samples                    
        print("sample set for " + c + " is " + str(len(project_sample_set)))

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

                        project_name = program_name+"_"+c
            query = '{project(name:\"%s\"){id}}' % project_name
            fetched_project_id = submitter.query(query)["data"]["project"][0]["id"]

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
        
        
        
        
        




