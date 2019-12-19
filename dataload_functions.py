import gen3
from gen3 import submission
from gen3 import auth
import pandas as pd
import json
from requests.auth import AuthBase
import requests
import hashlib

from settings import APIURL, HEADERS

Gen3Submission = submission.Gen3Submission
endpoint = "https://gen3test.lisanwanglab.org"
auth = auth.Gen3Auth(endpoint, refresh_file="credentials.json")

submitter = Gen3Submission(endpoint, auth)

##smart uppercasing of lowercase phenotype values 
def phenotype_prettifier(rawInput):
    connectives = ['and', 'or']
    word_list = []

    def checkForSlash(input):
        if "/" in input:
            i = input.find("/")+1
            t = input[:i].lower() + input[i:].capitalize()
            build(t)

        else:
            build(input)

    def build(input):
        for word in input.split():
            if word not in connectives and word is not 'na':
                first = word[0].capitalize()
                rest = word[1:]
                word_list.append(first+rest)

            else:
                if word is not 'na':
                    word_list.append(word)
                else:
                    word_list.append('NA')

    checkForSlash(rawInput)
    return " ".join(word_list)


def build_dataset_url(dataset_name):
    dataset_url_base = "https://dss.naigads.org/datasets/"

    if " " in dataset_name:
        dn = dataset_name.replace(" ", "-")
        return dataset_url_base + dn.lower() + "/"
    else:
        return dataset_url_base + dataset_name.lower()


def datasetReport(consents, program_name, filesAndPhenotypes, samplesAndSubjects):
    sampleFiles = filesAndPhenotypes[0]
    nonSampleFiles = filesAndPhenotypes[1]
    allConsentFiles = filesAndPhenotypes[2]
    phenotypes = filesAndPhenotypes[3]

    print('{} consent-levels in this dataset').format(str(len(consents)))
    print('{} subjects with samples in this dataset').format(str(len(samplesAndSubjects)))
    print('{} individual level files in this dataset').format(str(len(sampleFiles)))
    print('{} non-sample files in this dataset').format(str(len(nonSampleFiles)))
    print('{} all-consent files in this dataset').format(str(len(allConsentFiles)))
    print('{} phenotype datapoints in this dataset').format(str(len(phenotypes)))

##returns array of arrays: fileSamp, nonSamp, allCon, phenotypes
def getFilesPhenotypes(dss_dataset_id): 
    filesSamples = getData(dss_dataset_id, "fileSamples")
    filesNonSamples = getData(dss_dataset_id, "fileNonSamples")
    filesAllConsent = getData(dss_dataset_id, "fileAllConsents")
    phenotypes = getData(dss_dataset_id, "phenotypes")

    return [filesSamples, filesNonSamples, filesAllConsent, phenotypes]

## returns sampleDict (with subject info `included`)
def getSamplesSubjects(dss_dataset_id):
    print( APIURL+"datasetVersions/"+str(dss_dataset_id)+"/sampleSets" )
    response = requests.get(APIURL+"datasetVersions/"+str(dss_dataset_id)+"/sampleSets", headers=HEADERS)
    ## a list of sample sets for a dataset
    sample_set_data = response.json()["data"]
    
    sample_dict = {}

    for sample_set in sample_set_data:    
        ## have to check first that there are samples in the sampleSet
        request_url = APIURL+"sampleSets/"+str(sample_set["id"])+"/samples"
        print('checking to see if there are samples from files from ' + request_url)
        response = requests.get(request_url, headers=HEADERS)
        if len(response.json()["data"]) > 0:

            request_url = APIURL+"sampleSets/"+str(sample_set["id"])+"/samples?includes=subject.fullConsent&per_page=1000"
            print( 'getting samples from ' + request_url )
            response = requests.get(request_url, headers=HEADERS)  

            last_page = response.json()["meta"]["last_page"]
            sample_set_subject_data = response.json()["data"]
            
            print( "api returning " + str(last_page) + " page(s) of samples for sampleset " + str(sample_set["id"]) ) 
            for sample in sample_set_subject_data:
                sample_dict[sample["key"]] = sample

            if last_page > 1:
                for page in range( last_page + 1 ):
                    if page < 2:
                        continue
                    else:
                        response = requests.get(request_url + "&page=" + str(page), headers=HEADERS)
                        print('Retrieving samples from ' + request_url + "&page=" + str(page))
                        sample_set_subject_data = response.json()["data"]

                        for sample in sample_set_subject_data:
                            sample_dict[sample["key"]] = sample
                            
        else:
            print('no samples in this sampleSet. Moving on...')
            continue
    
        print(str(len(sample_dict)) + " subjects currently in this dataset")
    
    return sample_dict

def getConsents(dss_dataset_id):
    dataset_consents = []

            ##tested versioning-compliant url = https://dev3.niagads.org/darm/api/datasetVersions/{id}/consents
    request_url = APIURL+"datasetVersions/"+str(dss_dataset_id)+"/consents"
    response = requests.get(request_url, headers=HEADERS)
    print('Checking for datasets consent levels')
    last_page = response.json()["meta"]["last_page"]
    consent_data = response.json()["data"]
    ## gets the data from first return regardless of need for paginated retrieval
    for consent in consent_data:
        dataset_consents.append(consent["key"])

    if last_page > 1:
        for page in range( last_page + 1 ):
            if page < 2:
                continue
            else:
                response = requests.get(request_url + "?page=" + str(page), headers=HEADERS)
                print('getting paginated data from ' + request_url + "?page=" + str(page))
                consent_data = response.json()["data"]
                for consent in consent_data:
                    dataset_consents.append(consent["key"])

    print(str(len(dataset_consents)) + " consent(s) where found for datasetVersion " + str( dss_dataset_id ) )

    return dataset_consents


def getData(dss_dataset_id, filetype):
    data_list = []
 
    if 'file' in filetype:
        request_url = APIURL+"datasetVersions/"+str(dss_dataset_id)+"/"+filetype
    elif filetype == 'phenotypes':
        request_url = APIURL+"datasetVersions/"+str(dss_dataset_id)+"/phenotypeSets/6/subjectPhenotypes?includes=phenotype,subject&per_page=11000"

    print('checking to see if there are data from ' + request_url)
    response = requests.get(request_url, headers=HEADERS)
    if len(response.json()["data"]) > 0:
        if filetype == 'fileSamples':
            request_url = APIURL+"datasetVersions/"+str(dss_dataset_id)+"/"+filetype+"?includes=sample.subject.fullConsent&per_page=10000"
        elif 'file' in filetype:
            request_url = APIURL+"datasetVersions/"+str(dss_dataset_id)+"/"+filetype+"?per_page=1000"

        print( 'getting ' + filetype + ' data from ' + request_url )
        response = requests.get(request_url, headers=HEADERS)
        last_page = response.json()["meta"]["last_page"]
        returned_data = response.json()["data"]

        print( "api returning " + str(last_page) + " page(s) of " + filetype + " data." ) 
        for datum in returned_data:
            data_list.append(datum)
            
        if last_page > 1:
            for page in range( last_page + 1 ):
                if page < 2:
                    continue
                else:
                    response = requests.get(request_url + "&page=" + str(page), headers=HEADERS)
                    print('getting paginated data from ' + request_url + "&page=" + str(page))
                    returned_data = response.json()["data"]

                    for datum in returned_data:
                        data_list.append(datum)
                
                print('length of data_list after page ' + str(page) + ": " + str(len(data_list)))
    else:
        print('no ' + filetype + ' data, moving on...')

    print(str(len(data_list)) + " " + filetype + " data found for datasetVersion " + str(dss_dataset_id))
    return data_list

def createProject(consent, program_name, filesAndPhenotypes, samplesAndSubjects):

    print('Creating project for {}, consent-level {}').format(program_name, consent)

    project_sample_set = set({})
    for key, sample in samplesAndSubjects.iteritems():
        ## some subjects have 'null' consent, ignoring for now
        if sample["subject"]["consent"] is not None:
            ## if the subject's consent matches the current project, add to the pss set
            if sample["subject"]["consent"]["key"].strip() == consent:
                project_sample_set.add( sample["subject"]["key"] )

    print('creating project, {} unique subject ids found').format(str(len(project_sample_set)))

    if len(project_sample_set) > 0:
        project_name = program_name+"_"+consent
        project_obj = {
            "type": "project",
            "dbgap_accession_number": project_name,
            "name": project_name,
            "code": project_name,
            "availability_type": "Restricted"
        }
        print( "Creating project node for " + project_name )
        submitter.create_project(program_name, project_obj)

        ## create CMC for each created project

        query = '{project(name:\"%s\"){id}}' % project_name
        fetched_project_id = submitter.query(query)["data"]["project"][0]["id"]

        cmc_obj = {
            "*collection_type": "Consent-Level File Manifest", 
            "description": "Core Metadata Collection for "+project_name, 
            "type": "core_metadata_collection", 
            "submitter_id": project_name+"_"+"core_metadata_collection",
            "projects": {
                "id": fetched_project_id
            }
        }

        print( "creating core_metadata_collection for " + project_name )
        submitter.submit_record(program_name, project_name, cmc_obj)

        createSubjectAndAssociated(project_sample_set, filesAndPhenotypes, program_name, project_name, consent)

def createSubjectAndAssociated(project_sample_set, filesAndPhenotypes, program_name, project_name, consent):
