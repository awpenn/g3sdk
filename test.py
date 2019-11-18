import gen3
from gen3 import submission
from gen3 import auth
import pandas as pd
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

## transform/builder function 
def apoe_tranform(pnode):
    accepted_values = ["22", "23", "24", "33", "34", "44", "na"]
    if pnode["value"] in accepted_values:
        return pnode["value"]
    else:
        return 'na'

def sex_transform(pnode):
    # print(pnode)
    p = pnode["value"]
    p_dict = json.loads(pnode["phenotype"]["values"])
    accepted_values = ["male", "female"]

    return p    

def race_transform(pnode):

    p = pnode["value"]
    p_dict = json.loads(pnode["phenotype"]["values"])

    accepted_values = ["american indian/alaska native", "asian", "black or african american", "native hawaiian or other pacific islander", "other", "white", "na"]


    return p

def ethnicity_transform(pnode):

    p = pnode["value"]
    p_dict = json.loads(pnode["phenotype"]["values"])

    accepted_values = ["hispanic or latino", "not hispanic or latino", "not applicable/not available"]
    
    return p

def dx_transform(pnode):
    p = pnode["value"]
    p_dict = json.loads(pnode["phenotype"]["values"])

    accepted_values = [
                    "case",
                    "control",
                    "other",
                    "unknown"
                ]

    if "ad" in p:
        dv = 'case'
    else:
        dv = 'unknown'
    
    return dv

def disease_transform(pnode):
    p = pnode["value"]
    p_dict = json.loads(pnode["phenotype"]["values"])
    
    return p

def build_dataset_url(dataset_name):
    dataset_url_base = "https://dss.naigads.org/datasets/"

    if " " in dataset_name:
        dn = dataset_name.replace(" ", "-")
        return dataset_url_base + dn.lower() + "/"
    else:
        return dataset_url_base + dataset_name.lower()


## how to add to base string
## APIURL+urltail"/1/subjects 

response = requests.get(APIURL+urltail, headers=headers)
# response.json() produces a dictionary
dataset_data = response.json()['data']
for dataset in dataset_data:
    program_release_name = dataset["name"]
    program_dbgap = dataset["accession"]
    program_name = dataset["accession"]
    program_description = dataset["description"]
    dss_dataset_id = dataset["id"]
    program_url = build_dataset_url(program_release_name)
    
    ## AW- production will have latest_release as a variable
    program_obj = {
        "type": "program",
        "dbgap_accession_number": program_dbgap,
        "name": program_name,
        "release_name": program_release_name,
        "summary_description": program_description,
        "dataset_url": program_url, 
    }

    ## create programs from dataset list
    print( "creating program node for " + program_dbgap )
    print(program_obj)
    submitter.create_program(program_obj)
    ## get guid for program based on program_name, store as fetched_id to link subjects, filesets, core_metadata_collections
    query = '{program(name:\"%s\"){id}}' % program_name

    fetched_program_id = submitter.query(query)["data"]["program"][0]["id"]
    # ## get all the filesets for a dataset, to be used later
    urltail = 'datasets'
    # request_url = APIURL+urltail+"/"+str(dss_dataset_id)+"/filesets"
    # print('Getting fileset data from ' + request_url)
    # response = requests.get(request_url, headers=headers)
    # fileset_data = response.json()["data"]
    
    # # make lists with fileset sample non-sample, and all-con files
    # fileset_sample_files_list = []
    # fileset_nonsample_files_list = []
    # fileset_allconsents_files_list = []


    # for fileset in fileset_data:
    #     urltail = 'filesets'

    #     ## get sample-related files
    #     request_url = APIURL+urltail+"/"+str(fileset["id"])+"/"+"fileSamples?includes=sample.subject.fullConsent&per_page=1000"
    #     print( 'getting sample files from ' + request_url )
    #     response = requests.get(request_url, headers=headers)
    #     last_page = response.json()["meta"]["last_page"]
    #     fileset_sample_data = response.json()["data"]

    #     for file in fileset_sample_data:
    #         fileset_sample_files_list.append(file)
        
    #     if last_page > 1:
    #         for page in range( last_page + 1 ):
    #             if page < 2:
    #                 continue
    #             else:
    #                 response = requests.get(request_url + "&page" + str(page), headers=headers)
    #                 fileset_sample_data = response.json()["data"]
    #                 for sample_file in fileset_sample_data:
    #                     fileset_sample_files_list.append(sample_file)

    #     ## get non-sample files
    #     request_url = APIURL+urltail+"/"+str(fileset["id"])+"/"+"fileNonSamples&per_page=1000"
    #     print( 'getting non-sample files from ' + request_url )
    #     response = requests.get(request_url, headers=headers)
    #     last_page = response.json()["meta"]["last_page"]
    #     fileset_nonsample_data = response.json()["data"]

    #     for file in fileset_nonsample_data:
    #         fileset_nonsample_files_list.append(file)
       
    #     if last_page > 1:
    #         for page in range( last_page + 1 ):
    #             if page < 2:
    #                 continue
    #             else:
    #                 response = requests.get(request_url + "&page" + str(page), headers=headers)
    #                 fileset_non_sample_data = response.json()["data"]
    #                 for non_sample_file in fileset_non_sample_data:
    #                     fileset_non_sample_files_list.append(non_sample_file)

    #     ## get all-con files
    #     request_url = APIURL+urltail+"/"+str(fileset["id"])+"/"+"fileAllConsents&per_page=1000"
    #     print( 'getting all-consent files from ' + request_url )
    #     response = requests.get(request_url, headers=headers)
    #     last_page = response.json()["meta"]["last_page"]
    #     fileset_allconsents_data = response.json()["data"]

    #     for file in fileset_allconsents_data:
    #         fileset_allconsents_files_list.append(file)
        
    #     if last_page > 1:
    #         for page in range( last_page + 1 ):
    #             if page < 2:
    #                 continue
    #             else:
    #                 response = requests.get(request_url + "&page" + str(page), headers=headers)
    #                 fileset_allconsents_data = response.json()["data"]
    #                 for all_consents_file in fileset_allconsents_data:
    #                     fileset_allconsents_files_list.append(all_consents_file)

    ## get all the phenotype nodes for a dataset, to be entered when subject/sample nodes created below
    urltail = 'datasets'
    request_url = APIURL+urltail+"/"+str(dss_dataset_id)+"/subjectPhenotypes?includes=phenotype,subject&per_page=11000"
    response = requests.get(request_url, headers=headers)
    last_page = response.json()["meta"]["last_page"]
    phenotype_data = response.json()["data"]

    ## this list is all the phenotype nodes from all the pages
    print( "creating phenotype list for dataset " + str(dss_dataset_id) )
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
        sample_set_subject_data = response.json()["data"]
        ## merges multiple dictionaries into one list of dictionaries
        for sample in sample_set_subject_data:
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
            ## this loop will create all subject, phenotype, and samples for a particular project
            for dictkey, value in enumerate(project_sample_set):
                ## AW - why doesn't this create two subjects for subjects with multiple samples...but if tied to same subject entity (with submitter id) in db then maybe overwrites with same info?
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
                        print( "creating subject record " + subject["key"] )
                        submitter.submit_record(program_name, project_name, subject_obj)

                        ## create a sample node for each passing through here while we're at it
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

                        current_subject_id = subject["key"]
                        current_subject_phenotypes_dict = {}
                        
                        for pnode in project_phenotype_list:
                            if pnode["subject"]["key"] == current_subject_id:
                                # print(pnode["phenotype"]["name"]+": "+pnode["value"]) = phenotype: phenotype value
                                ## these are our five 'core-harmonized' phenotypes that need to be sought out

                                if pnode["phenotype"]["name"].lower() == "apoe":
                                    current_subject_phenotypes_dict["apoe"] = apoe_tranform(pnode)
                                
                                if pnode["phenotype"]["name"].lower() in ["sex", "gender"]:
                                    current_subject_phenotypes_dict["sex"] = sex_transform(pnode)

                                if pnode["phenotype"]["name"].lower() in ["race"]:
                                    current_subject_phenotypes_dict["race"] = race_transform(pnode)

                                if pnode["phenotype"]["name"].lower() in ["ethnicity"]:
                                    current_subject_phenotypes_dict["ethnicity"] = ethnicity_transform(pnode)

                                if pnode["phenotype"]["name"].lower() in ["dx", "diagnosis"]:
                                    current_subject_phenotypes_dict["study_specific_diagnosis"] = dx_transform(pnode)

                                current_subject_phenotypes_dict["disease"] = "AD"

                        phenotype_obj = {
                            "APOE": current_subject_phenotypes_dict["apoe"], 
                            "sex": current_subject_phenotypes_dict["sex"], 
                            "subjects": {
                                "submitter_id": current_subject_id
                            }, 
                            "race": current_subject_phenotypes_dict["race"], 
                            "type": "phenotype", 
                            "study_specific_diagnosis": current_subject_phenotypes_dict["study_specific_diagnosis"], 
                            "disease": current_subject_phenotypes_dict["disease"], 
                            "submitter_id": current_subject_id + "_pheno", 
                            "ethnicity": current_subject_phenotypes_dict["ethnicity"]
                        }

                        print("creating phenotype record for " + current_subject_id)
                        print(phenotype_obj)
                        submitter.submit_record(program_name, project_name, phenotype_obj)
            ## once all the subject, sample, and phenotype records for a project are created, create a fileset_[consent_level]
            ## node corresponding to current project (consent level) for each fileset in the program (dataset), query for the files
            ## of each using conditional based on current consent ("c in dataset current loop") and the fileset query's
            ## return data's `sample.subject.consent.key`, create file nodes linked to sample based on `sample.key` and
            ## to fileset based on generated fileset GUID, [filsetaccno+type+filePK]

            ## program-level fileset data was retrieved for dataset above as `fileset_data` so don't have to query for each consent
            # print('Creating filesets for ' + project_name)
            # for fileset in fileset_data:
            #     fileset_id = fileset["id"]
            #     fileset_description = fileset["description"]
            #     # # accession not yet in API data so will fake
            #     # # fileset_name = fileset.accession + "_" + c
            #     fileset_name = fileset["accession"]+"_"+c
            #     # # accession not yet in API data so will fake
            #     # # fileset_submitter_id = fileset.accession + "_" + c
            #     fileset_submitter_id = fileset["accession"]+"_"+c

            #     fileset_obj =  {
            #         "*projects": {
            #         "id": fetched_project_id
            #         }, 
            #         "*description": fileset_description, 
            #         "*fileset_name": fileset_name, 
            #         "*type": "fileset", 
            #         "*submitter_id": fileset_submitter_id
            #     }
            #     print(fileset_obj)
            #     submitter.submit_record(program_name, project_name, fileset_obj)
            
            #     ## Get sample-related files for each fileset while creating, 
            #     ## first filtering on fileset_id (in fileset_sample_files_list object) == fileset_id
            #     ## then by c (current consent)
            #     for file in fileset_sample_files_list:
            #         if file["fileset_id"] == fileset_id and file["subject"]["consent"]["key"] == c:
            #             ##in DSS type=cram, index, etc., on datastage that is format
            #             file_format = file["type"]
            #             ##in datastage this is WGS, WES, etc., which is sample.assay in dss data
            #             file_type = file["sample"]["assay"]
            #             file_path = file["path"]
            #             file_name = file["name"]
            #             file_size = file["size"]
            #             file_id = file["id"]
            #             sample_key = file["sample"]["key"]
            #             cmc_submitter_id = project_name+"_core_metadata_collection"
            #             file_submitter_id = file_name + "_" + file_format + "_" + str(file_id)
            #             file_md5 = hashlib.md5( file_name + file_format + str(file_id) ).hexdigest()
                        
            #             ## AW- currently missing ref_build and data_category(genotype, expression, etc.) because not in DSS data
             
            #             idlf_obj = {
            #                 "*data_type": file_type, 
            #                 "filesets": {
            #                 "submitter_id": fileset_submitter_id
            #                 }, 
            #                 "*consent": c, 
            #                 "core_metadata_collections": {
            #                 "submitter_id": cmc_submitter_id
            #                 }, 
            #                 "*type": "individual_level_data_file", 
            #                 "*file_path": file_path, 
            #                 "*data_format": file_format, 
            #                 "*file_name": file_name, 
            #                 "*md5sum": file_md5, 
            #                 "*file_size": file_size, 
            #                 "*samples": {
            #                 "submitter_id": sample_key
            #                 }, 
            #                 "*submitter_id": file_submitter_id
            #             }
            #             print("creating record for individual-related file:  " + file_submitter_id )
            #             submitter.submit_record(program_name, project_name, ildf_obj)

            #     ## Get non-sample-related files for each fileset while creating, 
            #     ## first filtering on fileSetId (in fileset_nonsample_files_list object) == fileset_id
            #     ## then by c (current consent) == consent_key in the list
            #     for file in fileset_nonsample_files_list:
            #         if file["fileSetId"] == fileset_id and file["consent_key"] == c:
            #             ##in DSS type=cram, index, etc., on datastage that is format
            #             ##file_type = ???? not in data (WGS WES etc.)... n/a for now
            #             file_type = 'n/a'
            #             file_size = file["size"]
            #             file_path = file["path"]
            #             file_name = file["name"]
            #             file_format = file["type"]
            #             file_id = file["id"]
            #             cmc_submitter_id = project_name + "_core_metadata_collection"
            #             file_submitter_id = file_name + "_" + file_format + "_" + str(file_id)
            #             file_md5 = hashlib.md5( file_name + file_format + str(file_id) ).hexdigest()

                         ## AW- currently missing data_type, ref_build, data_category(genotype, expression, etc.) because not in DSS data
            #             aldf_object = {
            #                 "*data_type": file_type, 
            #                 "filesets": {
            #                     "submitter_id": fileset_submitter_id
            #                 }, 
            #                 "*consent": c, 
            #                 "core_metadata_collections": {
            #                     "submitter_id": cmc_submitter_id
            #                 }, 
            #                 "*type": "aggregate_level_data_file", 
            #                 "*file_path": file_path, 
            #                 "*data_format": file_format, 
            #                 "*md5sum": file_md5, 
            #                 "*file_size": file_size, 
            #                 "*submitter_id": file_submitter_id, 
            #                 "*file_name": file_name
            #             }
                        

            #             print("creating record for non-sample file:  " + file_submitter_id )
            #             submitter.submit_record(program_name, project_name, aldf_obj)

            #     # Get all-con files for each fileset while creating, 
            #     # first filtering on fileset_id (in fileset_allconsents_files_list object) == fileset_id
            #     for file in fileset_allconsents_files_list:
            #         if file["fileset_id"] == fileset_id:
            #             file_format = file["type"]
            #             ##in datastage this is WGS, WES, etc., for allcons not in data and maybe not applicable, for now n/a
            #             file_type = 'n/a'
            #             file_path = file["path"]
            #             file_name = file["name"]
            #             file_size = file["size"]
            #             file_id = file["id"]
            #             cmc_submitter_id = project_name + "_core_metadata_collection"
            #             file_submitter_id = file_name + "_" + file_format + "_" + str(file_id)
            #             file_md5 = hashlib.md5( file_name + file_format + str(file_id)).hexdigest()
                            
                         ## AW- currently missing data_type, ref_build, data_category(genotype, expression, etc.) because not in DSS data

            #             aldf_obj = {
            #                 "*data_type": file_type, 
            #                 "filesets": {
            #                     "submitter_id": fileset_submitter_id
            #                 }, 
            #                 "*consent": c, 
            #                 "core_metadata_collections": {
            #                     "submitter_id": cmc_submitter_id
            #                 }, 
            #                 "*type": "aggregate_level_data_file", 
            #                 "*file_path": file_path, 
            #                 "*data_format": file_format, 
            #                 "*md5sum": file_md5, 
            #                 "*file_size": file_size, 
            #                 "*submitter_id": file_submitter_id, 
            #                 "*file_name": file_name
            #                 }

            #             print("creating record for all-consent file:  " + file_submitter_id )
            #             submitter.submit_record(program_name, project_name, aldf_obj)
                    