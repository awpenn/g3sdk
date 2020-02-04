from dataload_functions import *
import json
from requests.auth import AuthBase
import requests

from settings import APIURL, HEADERS


def get_datasets():
    response = requests.get(APIURL+"datasets?includes=datasetVersions", headers=HEADERS)
    dataset_data = response.json()['data']

    datasets_and_consents = []

    for dataset in dataset_data:
        accession_no = dataset["accession"]

        for version in dataset["datasetVersions"]:
            if version["active"] == 1:
                dss_dataset_id = version["id"]
        
        version_consents = getConsents(dss_dataset_id)

        datasets_and_consents.append( [accession_no, version_consents] )

    return datasets_and_consents

def build_resource_descriptions(dc): 
    for dataset in dc:
        program_name = dataset[0]
        projects = dataset[1]

        """make a resource object in the resource section along the lines of `- name: program_name`"""
        subresource_obj = {
            "name": program_name,
            "subresources": [
                {
                "name": "projects",
                "subresources": []               
                }
            ]
        }
        if len(projects) > 0:
            """add `subresources` block"""
            for project in projects:
                project_name = program_name + "_" + project
                """add to subresources `-{name: project_name}"""
                subresource_obj["subresources"][0]["subresources"].append( {"name": project_name } )
        
        template["rbac"]["resources"][0]["subresources"].append(subresource_obj)
    
    """just used to test well-formedness"""
    print('program/project descriptions created')
    # write_to_file("testuserbuild", template)

"""currently, api doesn't support getting a list of users, and if we have users with no applications (?) then no need to add them anyway"""
def get_users_and_apps():
    """aw - will have to change to paginated collection as elsewhere but fine for now"""
    subroute = 'applications?per_page=500'      
    requrl = APIURL + subroute
    response = requests.get(requrl, headers=HEADERS)
    all_applications = response.json()['data']

    active_users = set()
    for app in all_applications:
        user = app['user']['id']
        email = app['user']['email']

        user_tup = (user, email)

        active_users.add(user_tup)
    
    return [list(active_users), all_applications]

def build_user_permissions(users_and_apps):
    users = users_and_apps[0]
    apps = users_and_apps[1]

    programs = template["rbac"]["resources"][0]["subresources"] ## change template_plus_... to just template after testing

    for user in users:
        user_id = user[0]
        email = user[1]

        """ultimately added to template as `template["users"][email] = user_obj , where email is the key (template["users"] is a dict)"""
        user_obj = {
            "admin": "false",
            "projects": []
        }
        """make a program resource for each existing, so can see aggs but still restricted on subject info access"""
        for program in programs:
            program_obj = {
                "auth_id": program["name"],
                "privilege": [
                        "read"
                ], 
                "resource": "/programs/" + program["name"]
            }
            user_obj["projects"].append(program_obj)
        
        """removes duplicate permissions across applications"""
        resource_set = set()

        for app in apps:
            if app["user"]["id"] == user_id:
                subroute = 'applications/' + str(app["id"]) + '/approvedConsents'     
                requrl = APIURL + subroute
                response = requests.get(requrl, headers=HEADERS)
                approved_consents = response.json()['data']

                """will be a list of tuples = dataset and consent to the iterate through for resources"""
                dataset_and_consent = [] 

                for approved_consent in approved_consents:
                    consent = approved_consent["consent"]["key"]
                    dataset_accession = approved_consent["dataset"]["accession"]
                    dataset_and_consent.append( (dataset_accession, consent) )
                

                for resource_tuple in dataset_and_consent:
                    program = resource_tuple[0]
                    project = program + "_" + resource_tuple[1]
                    resource_path = "/programs/" + program + "/projects/" + project
                    
                    resource_set.add(resource_path)

        for resource_path in resource_set:
            delim = "/projects/"
            delimlen = len(delim)
            slindex = resource_path.index(delim)
            auth_id = resource_path[ slindex + delimlen:]

            project_obj = {
                "auth_id": auth_id,
                "privilege": [
                    "read"
                ],
                "resource": resource_path
            }

            user_obj["projects"].append(project_obj)


        template["users"][email] = user_obj ## change this to `template` after testing
    
def write_to_file(filename, data):
    with open("jsondumps/%s.json" % filename, "w") as outfile:
        """below, data from DSS api requires response.json() , from datastage = response"""
        json.dump(data, outfile)

def read_from_file(filename):
    with open("jsondumps/%s.json" % filename, "r") as readfile:
        data = json.load(readfile)
        return data

def open_template():
    with open("jsondumps/user-template.json", "r") as template_file:
        template = json.load(template_file)
        return template

def build_yaml(template):
    """convert json to yaml"""

    """for now 2/4 just going to write final product here"""
    write_to_file("user", template)


if __name__ == "__main__":

    template = open_template()

    datasets = get_datasets()

    build_resource_descriptions(datasets)
    
    """returns a set of ids and emails (may need to be changed to eRA id or whatever in the future) for users that have applications, as well as all the applications"""
    users_and_apps = get_users_and_apps()

    build_user_permissions(users_and_apps)

    build_yaml(template)