from dataload_functions import *

import gen3
from gen3 import submission
from gen3 import auth
import pandas as pd
import json
from requests.auth import AuthBase
import requests
import hashlib

import multiprocessing
from subprocess import call

from settings import APIURL, HEADERS

Gen3Submission = submission.Gen3Submission
endpoint = "https://gen3test.lisanwanglab.org"
auth = auth.Gen3Auth(endpoint, refresh_file="credentials.json")

submitter = Gen3Submission(endpoint, auth)

response = requests.get(APIURL+"datasets?includes=datasetVersions", headers=HEADERS)
# response.json() produces a dictionary
print(APIURL+"datasets?includes=datasetVersions")
dataset_data = response.json()['data']

def populate_datastage():
    def openFiles():
    ## 12/19 opening file just for dev, so dont have to go through api call process to test building
        with open("jsondumps/samplesSubjects.json", "r") as json_file:
            samplesAndSubjects = json.load(json_file)

        with open("jsondumps/fileSamples.json", "r") as json_file:
            filesSamples = json.load(json_file)

        with open("jsondumps/fileNonSamples.json", "r") as json_file:
            filesNonSamples = json.load(json_file)

        with open("jsondumps/fileAllConsents.json", "r") as json_file:
            filesAllConsents = json.load(json_file)

        with open("jsondumps/phenotypes.json", "r") as json_file:
            phenotypes = json.load(json_file)
                
        filesAndPhenotypes = [filesSamples, filesNonSamples, filesAllConsents, phenotypes]
        
        return [filesAndPhenotypes, samplesAndSubjects]

    for dataset in dataset_data:
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
        
        ## to run with calls API, uncomment the two function calls below and comment out the `openFiles` and related functions
        # filesAndPhenotypes = getFilesPhenotypes(dss_dataset_id) ##array of arrays, fileSamp, nonSamp, allCon, phenotypes
        # samplesAndSubjects = getSamplesSubjects(dss_dataset_id) ## sampleDict (with subject info `included`)
        consents = getConsents(dss_dataset_id)
        dataFromFiles = openFiles()

        filesAndPhenotypes = dataFromFiles[0]
        samplesAndSubjects = dataFromFiles[1]


        # datasetReport(consents, program_name, filesAndPhenotypes, samplesAndSubjects)

        print('Data to be loaded for dataset {}').format(program_name)
        print(str(len(samplesAndSubjects)))
        print(str(len(filesAndPhenotypes[0])))
        print(str(len(filesAndPhenotypes[1])))
        print(str(len(filesAndPhenotypes[2])))
        print(str(len(filesAndPhenotypes[3])))

        chunked_consents = list (partition(consents)) ## currently set to divide into groups of 3

        for chunk in chunked_consents:
            parallelArgs = []
            for consent in chunk:
                parallelArgs.append([consent, program_name, filesAndPhenotypes, samplesAndSubjects])
            ## runs three createProjects at once, early tests indicate marked speed increase (3.5 hrs vs 12)
            runInParallel(createProject, parallelArgs)
            
if __name__ == '__main__':
    populate_datastage()