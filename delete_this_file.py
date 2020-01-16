import sys
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




def run_phenotype_writer(consent="DS-ND-IRB-PUB"):
    phenotype_array = []

    def create_phenotype(current_subject_phenotypes_dict, current_subject_id):

        phenotype_obj = {
            "APOE": current_subject_phenotypes_dict["APOE"], 
            "sex": current_subject_phenotypes_dict["Sex"], 
            "subjects": {
                 "submitter_id": current_subject_id
            }, 
            "race": current_subject_phenotypes_dict["Race"], 
            "type": "phenotype", 
            "study_specific_diagnosis": current_subject_phenotypes_dict["Study_Specific_Diagnosis"], 
             "disease": current_subject_phenotypes_dict["Disease"], 
             "submitter_id": current_subject_id + "_pheno", 
            "ethnicity": current_subject_phenotypes_dict["Ethnicity"]
        }

        phenotype_array.append(phenotype_obj)

        
    project_sample_set = set({})
    with open("jsondumps/samplesSubjects.json", "r") as json_file:    
        samplesAndSubjects = json.load(json_file)

    with open("jsondumps/phenotypes.json", "r") as json_file:    
        phenotypes = json.load(json_file)


    for key, sample in samplesAndSubjects.iteritems():
        ## some subjects have 'null' consent, ignoring for now
        if sample["subject"]["consent"] is not None:
            ## if the subject's consent matches the current project, add to the pss set
            if sample["subject"]["consent"]["key"].strip() == consent:
                project_sample_set.add( sample["subject"]["key"] )
    
    for dictkey, subject_id in enumerate(project_sample_set):
        ## AW - why doesn't this create two subjects for subjects with multiple samples...but if tied to same subject entity (with submitter id) in db then maybe overwrites with same info?

        for samplekey, sample in samplesAndSubjects.iteritems():
            if sample["subject"]["key"] == subject_id:
                subject = sample["subject"]
                current_subject_id = subject["key"]

                current_subject_phenotypes_dict = {}

                for pnode in phenotypes:
                    
                    if pnode["subject"]["key"] == current_subject_id:
                        print('match ' + pnode["subject"]["key"])
                        ## pname = sex/race/etchnicity/etc.
                        ## may have to be appended with .lower() depending on how harmonized table turns out
                        pname = pnode["phenotype"]["name"]
                        ## index value given as phenotype value (0, 1, etc.)
                        p_index = str(pnode["value"])
                        ## json data dictionary attached to each phenotype
                        p_dict = json.loads(pnode["phenotype"]["values"])

                                ## makes a key/val pair in cspd dict dynamically, so don't need 6 if statements
                        current_subject_phenotypes_dict[pname] = phenotype_prettifier( p_dict[p_index] )
                
                create_phenotype(current_subject_phenotypes_dict, current_subject_id)

            if len(phenotype_array) > 0 and len(phenotype_array) % 5 == 0:
                print('-----')
                print(phenotype_array)
                # submitter.submit_record(program_name, project_name, sample_array)
                print('sending 3 phenotypes')
                phenotype_array = []
                print(phenotype_array)


def build_sample_file():
    subjectsAndSamples = getSamplesSubjects(1)
    print(len(subjectsAndSamples))


## building a 5 phenotype obj list of objects and manually dropping them into gen3 works 1/16 !!!if!!! there are no duplicate
##phenotype objects, but for some reason several times in tests there'll be a subjects phenotype data that gets duplicated,
## causes load failure for the list


##
"""subject and sample code below"""

def run_subject_writer(consent="HMB-IRB-PUB"):
    subject_array = []

    def create_subject(subject, current_subject_id):
        fetched_project_id = "218603c7-fe41-56e9-9ccd-f08daf002162"
        subject_obj = {
            "cohort": subject["cohort_key"], 
            "projects": {
                "id": fetched_project_id
            }, 
            "consent": subject["consent"]["key"], 
            "type": "subject", 
             "submitter_id": subject["key"]
        }
        subject_array.append(subject_obj)
        
    project_sample_set = set({})
    with open("jsondumps/samplesSubjects.json", "r") as json_file:    
        samplesAndSubjects = json.load(json_file)

    for key, sample in samplesAndSubjects.iteritems():
        ## some subjects have 'null' consent, ignoring for now
        if sample["subject"]["consent"] is not None:
            ## if the subject's consent matches the current project, add to the pss set
            if sample["subject"]["consent"]["key"].strip() == consent:
                project_sample_set.add( sample["subject"]["key"] )
    
    for dictkey, subject_id in enumerate(project_sample_set):
        ## AW - why doesn't this create two subjects for subjects with multiple samples...but if tied to same subject entity (with submitter id) in db then maybe overwrites with same info?

        for samplekey, sample in samplesAndSubjects.iteritems():
            if sample["subject"]["key"] == subject_id:
                subject = sample["subject"]
                current_subject_id = subject["key"]

                create_subject(subject, current_subject_id)

            if len(subject_array) > 0 and len(subject_array) % 5 == 0:
                print('-----')
                print(subject_array)
                # submitter.submit_record(program_name, project_name, sample_array)
                print('sending 5 samples')
                subject_array = []
                print(subject_array)


def build_sample_file():
    subjectsAndSamples = getSamplesSubjects(1)
    print(len(subjectsAndSamples))


## building a 5 subject list of objects and manually dropping them into gen3 works 1/16