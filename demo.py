project_phenotype_list = ## list of phenotype nodes for a dataset
current_subject_id = subject["key"] ## subject id of current subject being processed in loop
current_subject_phenotypes_dict = {}

for pnode in project_phenotype_list:
    if pnode["subject"]["key"] == current_subject_id:
        # print(pnode["phenotype"]["name"]+": "+pnode["value"]) = phenotype: phenotype value
        ## these are our five 'core-harmonized' phenotypes that need to be sought out

        if pnode["phenotype"]["name"].lower() == "apoe":
            current_subject_phenotypes_dict["apoe"] = apoe_tranform(pnode)
                                
        if pnode["phenotype"]["name"].lower() in ["sex", "gender"]:
            current_subject_phenotypes_dict["sex"] = sex_transform(pnode)

        if pnode["phenotype"]["other example"].lower() in ["another", "example"]:
            current_subject_phenotypes_dict["other example"] = another_transform(pnode)


print(current_subject_phenotypes_dict)

phenotype_obj = {
    "APOE": current_subject_phenotypes_dict["apoe"], 
    "sex": current_subject_phenotypes_dict["sex"], 
    "subjects": {
        "submitter_id": current_subject_id
    }, 
    "race": current_subject_phenotypes_dict["race"], 
    "type": "phenotype", 
    "diagnosis": current_subject_phenotypes_dict["dx"], 
    "submitter_id": current_subject_id + "_pheno", 
    "ethnicity": current_subject_phenotypes_dict["ethnicity"]
}

....
some code
.... 

def sex_transform(pnode):
    print('in sex transform')

    p = pnode["value"]
    ## derived value
    dv = ''
    accepted_values = ["male", "female"]
    p_dict = json.loads(pnode["phenotype"]["values"])

    if p_dict[str(p)] in accepted_values:
        dv = value.lower()

    else:
        dv = 'other/na'

    return dv   

example of test data

    {
        "created_at": "2019-09-19T18:59:00.000000Z",
        "subject_id": 3,
        "updated_at": "2019-09-19T18:59:00.000000Z",
        "value": "male", #in production will actually by 1 ?
        "phenotype_id": 1,
        "phenotype": {
            "description": "",
            "comments": "",
            "values": "{\"0\": \"female\", \"1\": \"male\"}",
            "internal_alias": "nisi",
            "id": 1,
            "name": "sex"
        },
        "id": 3,
        "subject": {
            "id": 3,
            "key": "A-ACT-AC000057",
            "cohort_key": "ACT"
        }
    },
