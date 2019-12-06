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

response = requests.get(APIURL+"datasets/", headers=headers)
# response.json() produces a dictionary
print(APIURL+APIURL+"/datasets")
dataset_data = response.json()['data']
for dataset in dataset_data:

    dss_dataset_id = dataset["id"]
    program_name = dataset["name"]
    program_url = build_dataset_url( program_name )
    
    ## AW- production will have latest_release as a variable
    program_obj = {
        "type": "program",
        "dbgap_accession_number": dataset["accession"],
        "name": dataset["accession"],
        "release_name": program_name,
        "summary_description": dataset["description"],
        "dataset_url": program_url, 
    }

    ## create programs from dataset list
    print( "creating program node for " + dataset["accession"] )

    submitter.create_program(program_obj)
    ## get guid for program based on program_name, store as fetched_id to link subjects, filesets, core_metadata_collections
    query = '{program(name:\"%s\"){id}}' % program_name

    fetched_program_id = submitter.query(query)["data"]["program"][0]["id"]
    # ## get all the filesets for a dataset, to be used later

    request_url = APIURL+"datasets/"+str(dss_dataset_id)+"/filesets"
    print('Getting fileset data from ' + request_url)
    response = requests.get(request_url, headers=headers)
    fileset_data = response.json()["data"]
    
    # make lists with fileset sample non-sample, and all-con files
    fileset_sample_files_list = []
    fileset_nonsample_files_list = []
    fileset_allconsents_files_list = []

    ## 12/6 I think here, with datasetVersion, all the files for a dataset across filesets are returned, 
    ## so we'll eliminate the fileset-based file aggregation (remove fileset_[filetype] from lists above)
    ## and then will have to remove match on fileset_id down in the ildf/ssdf writing section (because will be irrelevant)
    for fileset in fileset_data:

        ## get sample-related files
        ## have to see first if there are sample files
        request_url = APIURL+"filesets/"+str(fileset["id"])+"/fileSamples"
        print('checking to see if there are Sample files from ' + request_url)
        response = requests.get(request_url, headers=headers)
        if len(response.json()["data"]) > 0:
                    ##tested versioning-compliant url = https://dev3.niagads.org/darm/api/datasetVersions/{id}/fileSamples?includes=sample.subject.fullConsent
            request_url = APIURL+"filesets/"+str(fileset["id"])+"/fileSamples?includes=sample.subject.fullConsent&per_page=1000"
            print( 'getting sample files from ' + request_url )
            response = requests.get(request_url, headers=headers)
            last_page = response.json()["meta"]["last_page"]
            fileset_sample_data = response.json()["data"]

            for file in fileset_sample_data:
                fileset_sample_files_list.append(file)
            
            if last_page > 1:
                for page in range( last_page + 1 ):
                    if page < 2:
                        continue
                    else:
                        response = requests.get(request_url + "&page=" + str(page), headers=headers)
                        print('getting paginated data from ' + request_url + "&page=" + str(page))
                        fileset_sample_data = response.json()["data"]
                        for sample_file in fileset_sample_data:
                            fileset_sample_files_list.append(sample_file)
        else:
            print('no sample files, moving on...')

        ## get non-sample files
        ## have to see first if there are nonSample files
                    ##tested versioning-compliant url = https://dev3.niagads.org/darm/api/datasetVersions/{id}/fileNonSamples 
        request_url = APIURL+"filesets/"+str(fileset["id"])+"/fileNonSamples"
        print('checking to see if there are nonSample files from ' + request_url)
        response = requests.get(request_url, headers=headers)
        if len(response.json()["data"]) > 0:

            request_url = APIURL+"filesets/"+str(fileset["id"])+"/fileNonSamples?per_page=1000"
            print( 'getting non-sample files from ' + request_url )
            response = requests.get(request_url, headers=headers)

            last_page = response.json()["meta"]["last_page"]
            fileset_nonsample_data = response.json()["data"]

            for file in fileset_nonsample_data:
                fileset_nonsample_files_list.append(file)
        
            if last_page > 1:
                for page in range( last_page + 1 ):
                    if page < 2:
                        continue
                    else:
                        response = requests.get(request_url + "&page=" + str(page), headers=headers)
                        print('getting paginated data from ' + request_url + "&page=" + str(page))
                        fileset_non_sample_data = response.json()["data"]
                        for non_sample_file in fileset_non_sample_data:
                            fileset_non_sample_files_list.append(non_sample_file)
        else:
            print('no nonSample files, moving on...')
            continue

        ## get all-con files
        ## have to check first that there are all-con files
                    ##tested versioning-compliant url = https://dev3.niagads.org/darm/api/datasetVersions/{id}/fileAllConsents
        request_url = APIURL+"filesets/"+str(fileset["id"])+"/fileAllConsents"
        print('checking to see if there are nonSample files from ' + request_url)
        response = requests.get(request_url, headers=headers)
        if len(response.json()["data"]) > 0:
                    ##tested versioning-compliant url = https://dev3.niagads.org/darm/api/datasetVersions/{id}/fileAllConsents
            request_url = APIURL+"filesets/"+str(fileset["id"])+"/fileAllConsents?per_page=1000"
            print( 'getting all-consent files from ' + request_url )
            response = requests.get(request_url, headers=headers)
            last_page = response.json()["meta"]["last_page"]
            fileset_allconsents_data = response.json()["data"]

            for file in fileset_allconsents_data:
                fileset_allconsents_files_list.append(file)
            
            if last_page > 1:
                for page in range( last_page + 1 ):
                    if page < 2:
                        continue
                    else:
                        response = requests.get(request_url + "&page=" + str(page), headers=headers)
                        print('getting paginated data from ' + request_url + "&page=" + str(page))
                        fileset_allconsents_data = response.json()["data"]
                        for all_consents_file in fileset_allconsents_data:
                            fileset_allconsents_files_list.append(all_consents_file)
        else:
            print('no all-consent files, moving on...')
    
    print(str(len(fileset_sample_files_list)) + " sample file(s) retrieved total for dataset " + str(dss_dataset_id))
    print(str(len(fileset_nonsample_files_list)) + " nonsample file(s) retrieved total for dataset " + str(dss_dataset_id))
    print(str(len(fileset_allconsents_files_list)) + " allconsent file(s) retrieved total for dataset " + str(dss_dataset_id))
    print('-------fileset loop--------')
    ## get all the phenotype nodes for a dataset, to be entered when subject/sample nodes created below

    request_url = APIURL+"datasets/"+str(dss_dataset_id)+"/subjectPhenotypes?includes=phenotype,subject&per_page=11000"
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
                print('phenotypes from this string ' + request_url+"&page="+str(page))
                phenotype_data = response.json()["data"]
                for phenotype in phenotype_data:
                    project_phenotype_list.append(phenotype)

    ## get subjects/samples by querying sampleSets of a dataset with dss_dataset_id
                    
                    ##tested versioning-compliant url = https://dev3.niagads.org/darm/api/datasetVersions/{id}/sampleSets
    print( APIURL+"datasets/"+str(dss_dataset_id)+"/sampleSets" )
    response = requests.get(APIURL+"datasets/"+str(dss_dataset_id)+"/sampleSets", headers=headers)
    ## a list of sample sets for a dataset
    sample_set_data = response.json()["data"]
    
    sample_dict = {}
    print('these are in sample set data')
    print(sample_set_data)
    for sample_set in sample_set_data:    

        print('-----looping for one sample sets samples-----')
        ## have to check first that there are samples in the sampleSet
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
                sample_dict[sample["key"]] = sample

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
                            sample_dict[sample["key"]] = sample
                            
        else:
            print('no samples in this sampleSet. Moving on...')
            continue
    
        print(str(len(sample_dict)) + " subjects currently in this dataset")
    ## sample dict after this for loop will be all the samples for a dataset (some subjects have multi samples 4870 subj/4909 sample in dev server)
    ## sample_set is set of unique subject_ids in the sample_dict
    
    ## get a list of consents for the dataset
    dataset_consents = []

                        ##tested versioning-compliant url = https://dev3.niagads.org/darm/api/datasetVersions/{id}/consents
    response = requests.get(APIURL+"datasets/"+str(dss_dataset_id)+"/consents", headers=headers)

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

                                ## pname = sex/race/etchnicity/etc.
                                ## may have to be appended with .lower() depending on how harmonized table turns out
                                pname = pnode["phenotype"]["name"]
                                ## index value given as phenotype value (0, 1, etc.)
                                p_index = str(pnode["value"])
                                ## json data dictionary attached to each phenotype
                                p_dict = json.loads(pnode["phenotype"]["values"])

                                ## makes a key/val pair in cspd dict dynamically, so don't need 6 if statements
                                current_subject_phenotypes_dict[pname] = phenotype_prettifier( p_dict[p_index] )

                        print(current_subject_phenotypes_dict)
                        phenotype_obj = {
                            "APOE": current_subject_phenotypes_dict["apoe"], 
                            "sex": current_subject_phenotypes_dict["sex"], 
                            "subjects": {
                                "submitter_id": current_subject_id
                            }, 
                            "race": current_subject_phenotypes_dict["race"], 
                            "type": "phenotype", 
                            "study_specific_diagnosis": current_subject_phenotypes_dict["study_specific_diagnosis"], 
                            "disease": current_subject_phenotypes_dict["dx"], 
                            "submitter_id": current_subject_id + "_pheno", 
                            "ethnicity": current_subject_phenotypes_dict["ethnicity"]
                        }

                        print("creating phenotype record for " + current_subject_id)

                        submitter.submit_record(program_name, project_name, phenotype_obj)
            ## once all the subject, sample, and phenotype records for a project are created, create a fileset_[consent_level]
            ## node corresponding to current project (consent level) for each fileset in the program (dataset), query for the files
            ## of each using conditional based on current consent ("c in dataset current loop") and the fileset query's
            ## return data's `sample.subject.consent.key`, create file nodes linked to sample based on `sample.key` and
            ## to fileset based on generated fileset GUID, [filsetaccno+type+filePK]

            ## program-level fileset data was retrieved for dataset above as `fileset_data` so don't have to query for each consent
            print('Creating filesets for ' + project_name)
            for fileset in fileset_data:
                fileset_id = fileset["id"]
                fileset_description = ""
                if fileset["description"] is not None:
                    fileset_description = fileset["description"]

                fileset_obj =  {
                    "*projects": {
                    "id": fetched_project_id
                    }, 
                    "*description": fileset_description, 
                    "*fileset_name": fileset["accession"]+"_"+c, 
                    "*type": "fileset", 
                    "*submitter_id": fileset["accession"]+"_"+c
                }

                submitter.submit_record(program_name, project_name, fileset_obj)
            
                ## Get sample-related files for each fileset while creating, 
                ## first filtering on fileset_id (in fileset_sample_files_list object) == fileset_id
                ## then by c (current consent)

                for file in fileset_sample_files_list:

                    if file["sample"]["subject"]["consent"] is not None:
                        if file["fileset_id"] == fileset_id and file["sample"]["subject"]["consent"]["key"] == c:
                            ##in DSS type=cram, index, etc., on datastage that is data_format
                            ##in datastage, file_type = this is WGS, WES, etc., which is sample.assay in dss data

                            file_submitter_id = file_name + "_" + file_format + "_" + str( file["id"] )
                            file_md5 = hashlib.md5( file_name + file_format + str(file_id) ).hexdigest()
                            
                            ## AW- currently missing ref_build and data_category(genotype, expression, etc.) because not in DSS data
                
                            ildf_obj = {
                                "*data_type": file["sample"]["assay"], 
                                "filesets": {
                                "submitter_id": fileset_submitter_id
                                }, 
                                "*consent": c, 
                                "core_metadata_collections": {
                                "submitter_id": project_name+"_core_metadata_collection"
                                }, 
                                "*type": "individual_level_data_file", 
                                "*file_path": file["path"], 
                                "*data_format": file["type"], 
                                "*file_name": file["name"], 
                                "*md5sum": file_md5, 
                                "*file_size": file["size"], 
                                "*samples": {
                                "submitter_id": file["sample"]["key"]
                                }, 
                                "*submitter_id": file_submitter_id
                            }
                            print("creating record for individual-related file:  " + file_submitter_id )
                            submitter.submit_record(program_name, project_name, ildf_obj)

                ## Get non-sample-related files for each fileset while creating, 
                ## first filtering on fileSetId (in fileset_nonsample_files_list object) == fileset_id
                ## then by c (current consent) == consent_key in the list
                for file in fileset_nonsample_files_list:
                    if file["consent"] is not None:

                        if file["fileset_id"] == fileset_id and file["consent_key"] == c:
                            
                            ##in DSS type=cram, index, etc., on datastage that is format
                            ##file_type = ???? not in data (WGS WES etc.)... n/a for now
                            file_type = 'n/a'
                            file_format = file["type"]
                            file_id = file["id"]
                            file_submitter_id = file_name + "_" + file_format + "_" + str( file_id )
                            file_md5 = hashlib.md5( file["name"] + file_format + str( file_id ) ).hexdigest()

                            # AW- currently missing data_type, ref_build, data_category(genotype, expression, etc.) because not in DSS data
                            aldf_obj = {
                                "*data_type": file["type"], 
                                "filesets": {
                                    "submitter_id": fileset_submitter_id
                                }, 
                                "*consent": c, 
                                "core_metadata_collections": {
                                    "submitter_id": project_name + "_core_metadata_collection"
                                }, 
                                "*type": "aggregate_level_data_file", 
                                "*file_path": file["path"], 
                                "*data_format": file_format, 
                                "*md5sum": file_md5, 
                                "*file_size": file["size"], 
                                "*submitter_id": file_submitter_id, 
                                "*file_name": file["name"]
                            }
                            

                            print("creating record for non-sample file:  " + file_submitter_id )
                            submitter.submit_record(program_name, project_name, aldf_obj)

                # Get all-con files for each fileset while creating, 
                # first filtering on fileset_id (in fileset_allconsents_files_list object) == fileset_id
                for file in fileset_allconsents_files_list:
                    if file["fileset_id"] == fileset_id:

                        ##in datastage this is WGS, WES, etc., for allcons not in data and maybe not applicable, for now n/a
                        file_type = 'n/a'
                        file_format = file["type"]
                        file_id = file["id"]
                        file_submitter_id = file_name + "_" + file_format + "_" + str( file_id )
                        file_md5 = hashlib.md5( file_name + file_format + str( file_id )).hexdigest()
                            
                         # AW- currently missing data_type, ref_build, data_category(genotype, expression, etc.) because not in DSS data

                        aldf_obj = {
                            "*data_type": file_type, 
                            "filesets": {
                                "submitter_id": fileset_submitter_id
                            }, 
                            "*consent": c, 
                            "core_metadata_collections": {
                                "submitter_id": project_name + "_core_metadata_collection"
                            }, 
                            "*type": "aggregate_level_data_file", 
                            "*file_path": file["path"], 
                            "*data_format": file_format, 
                            "*md5sum": file_md5, 
                            "*file_size": file["size"], 
                            "*submitter_id": file_submitter_id, 
                            "*file_name": file["name"]
                            }

                        print("creating record for all-consent file:  " + file_submitter_id )
                        submitter.submit_record(program_name, project_name, aldf_obj)
                    