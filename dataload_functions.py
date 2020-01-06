import gen3
from gen3 import submission
from gen3 import auth
import pandas as pd
import json
from requests.auth import AuthBase
import requests
import hashlib

import calendar
import time

import threading
from multiprocessing import Process

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

def partition(consents):
    consents_per_chunk = 3
    for i in range(0, len(consents), consents_per_chunk):
        yield consents[i:i + consents_per_chunk]

def runInParallel(fn, args_list):
    proc = []

    for consent_args in args_list:
        p = Process(target=fn, args=[consent_args])
        p.start()
        proc.append(p)
    for p in proc:
        p.join()

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
    filesAllConsents = getData(dss_dataset_id, "fileAllConsents")
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
    with open("jsondumps/%s.json" % "samplesSubjects", "a") as outfile:
    # below, data from DSS api requires response.json() , from datastage = response
        json.dump(sample_dict, outfile)
    return sample_dict

def getConsents(dss_dataset_id):
    dataset_consents_unordered = []
    dataset_consents_tuples = []
    dataset_consents_ordered = []
    
            ##tested versioning-compliant url = https://dev3.niagads.org/darm/api/datasetVersions/{id}/consents
    request_url = APIURL+"datasetVersions/"+str(dss_dataset_id)+"/consents"
    response = requests.get(request_url, headers=HEADERS)
    print('Checking for datasets consent levels')
    last_page = response.json()["meta"]["last_page"]
    consent_data = response.json()["data"]
    ## gets the data from first return regardless of need for paginated retrieval
    for consent in consent_data:
        dataset_consents_unordered.append(consent["key"])

    if last_page > 1:
        for page in range( last_page + 1 ):
            if page < 2:
                continue
            else:
                response = requests.get(request_url + "?page=" + str(page), headers=HEADERS)
                print('getting paginated data from ' + request_url + "?page=" + str(page))
                consent_data = response.json()["data"]
                for consent in consent_data:
                    dataset_consents_unordered.append(consent["key"])

    print(str(len(dataset_consents_unordered)) + " consent(s) where found for datasetVersion " + str( dss_dataset_id ) )

    ## gets subject count for each, makes tuple so can order consent levels in ASC by the count
    for consent in dataset_consents_unordered:
        request_url = APIURL+"datasetVersions/"+str(dss_dataset_id)+"/subjects?consent_key:eq:="+consent
        response = requests.get(request_url, headers=HEADERS)
        consent_subject_count = response.json()["meta"]["total"]

        ctup = (consent_subject_count, consent)
        dataset_consents_tuples.append((consent_subject_count, consent))

    dataset_consents_tuples.sort()
    # dataset_consents_tuples.reverse()

    for consent_tuple in dataset_consents_tuples:
        dataset_consents_ordered.append(consent_tuple[1])
    
    return dataset_consents_ordered


def getData(dss_dataset_id, filetype):
    data_list = []
 
    if 'file' in filetype:
        request_url = APIURL+"datasetVersions/"+str(dss_dataset_id)+"/"+filetype
    elif filetype == 'phenotypes':
        request_url = APIURL+"datasetVersions/"+str(dss_dataset_id)+"/phenotypeSets/6/subjectPhenotypes?includes=phenotype,subject&per_page=9000"

    print('checking to see if there are data from ' + request_url)
    response = requests.get(request_url, headers=HEADERS)
    if len(response.json()["data"]) > 0:
        if filetype == 'fileSamples':
            request_url = APIURL+"datasetVersions/"+str(dss_dataset_id)+"/"+filetype+"?includes=sample.subject.fullConsent&per_page=9000"
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

    with open("jsondumps/%s.json" % filetype, "a") as outfile:
    # below, data from DSS api requires response.json() , from datastage = response
        json.dump(data_list, outfile)

    return data_list



## [consent, program_name, filesAndPhenotypes, samplesAndSubjects]
def createProject(arr):
    consent = arr[0]
    program_name = arr[1]
    filesAndPhenotypes = arr[2]
    filesSamples = filesAndPhenotypes[0]
    filesNonSamples = filesAndPhenotypes[1]
    filesAllConsents = filesAndPhenotypes[2]
    phenotypes = filesAndPhenotypes[3]
    samplesAndSubjects = arr[3]


    print('Creating project for {} in {}').format(consent, program_name)
    print('Starting project {} build at {}').format(consent, calendar.timegm(time.gmtime()))

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
        print(query)
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

        createSubjectsAndSamples(project_sample_set, samplesAndSubjects, phenotypes, program_name, project_name, consent, fetched_project_id)

        ## node generation with multiprocessing
        p1 = Process(target=createIDLFs, args=(consent, filesSamples, project_name, program_name))
        p2 = Process(target=createALDFs, args=(consent, filesNonSamples, project_name, program_name, "filesNonSamples"))
        p3 = Process(target=createALDFs, args=(consent, filesAllConsents, project_name, program_name, "filesAllConsents"))

        p1.start()
        p2.start()
        p3.start()
 
        p1.join()
        p2.join()
        p3.join()


        # createIDLFs(consent, filesSamples, project_name, program_name)
        # createALDFs(consent, filesNonSamples, project_name, program_name, "filesNonSamples")
        # createALDFs(consent, filesAllConsents, project_name, program_name, "filesAllConsents")

def createSubjectsAndSamples(project_sample_set, samplesAndSubjects, phenotypes, program_name, project_name, consent, fetched_project_id):
    for dictkey, subject_id in enumerate(project_sample_set):
        ## AW - why doesn't this create two subjects for subjects with multiple samples...but if tied to same subject entity (with submitter id) in db then maybe overwrites with same info?
        for samplekey, sample in samplesAndSubjects.iteritems():
            if sample["subject"]["key"] == subject_id:
                
                subject = sample["subject"]
                current_subject_id = subject["key"]

                def create_subject():
                    subject_obj = {
                        "cohort": subject["cohort_key"], 
                        "projects": {
                            "id": fetched_project_id
                        }, 
                        "consent": subject["consent"]["key"], 
                        "type": "subject", 
                        "submitter_id": subject["key"]
                    }

                    print( "creating subject record " + subject["key"] )
                    submitter.submit_record(program_name, project_name, subject_obj)

                def create_sample():
                    print( "creating sample record(s) for " + sample["key"] )

                    sample_obj = {
                        "platform": sample["platform"], 
                        "type": "sample", 
                        "submitter_id": sample["key"], 
                        "data_type": sample["assay"], 
                        "sample_source": sample["source"], 
                        "subjects": {
                            "submitter_id": sample["subject"]["key"]
                        }
                    }
                    # print(sample_obj) # for checking object correctness
                    submitter.submit_record(program_name, project_name, sample_obj)

                def create_phenotype():
                    current_subject_phenotypes_dict = {}
                            
                    for pnode in phenotypes:
                        if pnode["subject"]["key"] == current_subject_id:

                            ## pname = sex/race/etchnicity/etc.
                            ## may have to be appended with .lower() depending on how harmonized table turns out
                            pname = pnode["phenotype"]["name"]
                            ## index value given as phenotype value (0, 1, etc.)
                            p_index = str(pnode["value"])
                            ## json data dictionary attached to each phenotype
                            p_dict = json.loads(pnode["phenotype"]["values"])

                            ## makes a key/val pair in cspd dict dynamically, so don't need 6 if statements
                            current_subject_phenotypes_dict[pname] = phenotype_prettifier( p_dict[p_index] )

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

                    print("creating phenotype record for " + current_subject_id)

                    submitter.submit_record(program_name, project_name, phenotype_obj)

                create_subject()
                create_sample()
                create_phenotype()
                
def createIDLFs(consent, filesSamples, project_name, program_name):
    for file in filesSamples:
        if file["sample"]["subject"]["consent"] is not None:
            if file["sample"]["subject"]["consent"]["key"].strip() == consent:
                ##in DSS type=cram, index, etc., on datastage that is data_format
                ##in datastage, file_type = this is WGS, WES, etc., which is sample.assay in dss data
                file_name = file["name"]
                file_format = file["type"]
                file_id = file["id"]
                file_submitter_id = file_name + "_" + file_format + "_" + str( file["id"] )
                file_md5 = hashlib.md5( file_name + file_format + str(file_id) ).hexdigest()
                            
                        ## AW- currently missing ref_build and data_category(genotype, expression, etc.) because not in DSS data
                
                ildf_obj = {
                    "*data_type": file["sample"]["assay"], 
                    "*consent": consent, 
                    "core_metadata_collections": {
                    "submitter_id": project_name+"_core_metadata_collection"
                    }, 
                    "*type": "individual_level_data_file", 
                    "*file_path": file["path"], 
                    "*data_format": file_format, 
                    "*file_name": file_name, 
                    "*md5sum": file_md5, 
                    "*file_size": file["size"], 
                    "*samples": {
                    "submitter_id": file["sample"]["key"]
                    }, 
                    "fileset": file["fileset"]["accession"]
                    "*submitter_id": file_submitter_id
                }
                print("creating record for individual-related file:  " + file_submitter_id )

                submitter.submit_record(program_name, project_name, ildf_obj)

def createALDFs(consent, files_list, project_name, program_name, filetype):
    def create_non_sample_file():
        for file in files_list:
            if file["consent"] is not None:

                if file["consent_key"].strip() == consent:
                            
                    ##in DSS type=cram, index, etc., on datastage that is format
                    ##file_type = ???? not in data (WGS WES etc.)... n/a for now
                    file_type = 'n/a'
                    file_format = file["type"]
                    file_id = file["id"]
                    file_name = file["name"]
                    file_submitter_id = file_name + "_" + file_format + "_" + str( file_id )
                    file_md5 = hashlib.md5( file_name + file_format + str( file_id ) ).hexdigest()

                            # AW- currently missing data_type, ref_build, data_category(genotype, expression, etc.) because not in DSS data
                    aldf_obj = {
                        "*data_type": file["type"], 
                        "*consent": consent, 
                        "core_metadata_collections": {
                            "submitter_id": project_name + "_core_metadata_collection"
                        }, 
                        "*type": "aggregate_level_data_file", 
                        "*file_path": file["path"], 
                        "*data_format": file_format, 
                        "*md5sum": file_md5, 
                        "*file_size": file["size"], 
                        "*submitter_id": file_submitter_id,
                        "fileset": file["fileset"]["accession"] 
                        "*file_name": file_name
                    }
                            
                    print("creating record for non-sample file:  " + file_submitter_id )
                    submitter.submit_record(program_name, project_name, aldf_obj)
    
    def create_all_consent_file():
        for file in files_list:

            ##in datastage this is WGS, WES, etc., for allcons not in data and maybe not applicable, for now n/a
            file_type = 'n/a'
            file_format = file["type"]
            file_id = file["id"]
            file_name = file["name"]
            file_submitter_id = file_name + "_" + file_format + "_" + str( file_id )
            file_md5 = hashlib.md5( file_name + file_format + str( file_id )).hexdigest()
                            
            # AW- currently missing data_type, ref_build, data_category(genotype, expression, etc.) because not in DSS data

            aldf_obj = {
                "*data_type": file_type, 
                "*consent": consent, 
                "core_metadata_collections": {
                    "submitter_id": project_name + "_core_metadata_collection"
                }, 
                "*type": "aggregate_level_data_file", 
                "*file_path": file["path"], 
                "*data_format": file_format, 
                "*md5sum": file_md5, 
                "*file_size": file["size"], 
                "*submitter_id": file_submitter_id, 
                "*file_name": file_name
            }

            print("creating record for all-consent file:  " + file_submitter_id )
            submitter.submit_record(program_name, project_name, aldf_obj)

    if filetype == 'filesNonSamples':
        create_non_sample_file()
    elif filetype == 'filesAllConsents':
        create_all_consent_file()
