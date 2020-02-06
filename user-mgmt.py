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

"""currently, api doesn't support getting a list of users, and if we have users with no applications (?) then no need to add them anyway"""
def get_users_and_apps():
    """aw - will have to change to paginated collection as elsewhere but fine for now"""
    subroute = 'applications?application_scope=approved&per_page=500&includes=dar.downloaders'      
    requrl = APIURL + subroute
    response = requests.get(requrl, headers=HEADERS)
    last_page = response.json()["meta"]["last_page"]
    all_applications = response.json()['data']

    active_users = set()
    total_processed = []
    for app in all_applications:
        if app["active"] == True:
            user = app["user"]["id"]
            login = app["user"]["user_login"]

            user_tup = (user, login)

            active_users.add(user_tup)

            if app["dar"]["downloaders"]:
                for d in app["dar"]["downloaders"]:
                    downloader_id = d["user"]["id"]
                    downloader_login = d["user"]["user_login"]

                    downloader_tup = (downloader_id, downloader_login)
                    active_users.add(downloader_tup)

    if last_page > 1:
        for page in range( last_page + 1 ):
            if page < 2:
                continue
            else:
                response = requests.get(requrl + "&page=" + str(page), headers=HEADERS)
                print('getting paginated data from ' + requrl + "&page=" + str(page))
                returned_data = response.json()["data"]

                for datum in returned_data:
                    if app["active"] == True:
                        user = app["user"]["id"]
                        login = app["user"]["user_login"]

                        user_tup = (user, login)

                        active_users.add(user_tup)
    
    return [list(active_users), all_applications]

def build_user_permissions(users_and_apps):
    users = users_and_apps[0]
    apps = users_and_apps[1]

    programs = template["rbac"]["resources"][0]["subresources"] ## change template_plus_... to just template after testing

    for user in users:
        user_id = user[0]
        user_login = user[1]

        """ultimately added to template as `template["users"][user_login] = user_obj , where user_login is the key (template["users"] is a dict)"""
        user_obj = {
            "admin": "false",
            "projects": []
        }
        
        """removes duplicate permissions across applications"""
        resource_set = set()

        for app in apps:
            """collects downloader ids for the app in loop to check against current user in above loop"""
            downloader_id_list = []
            if app["dar"]["downloaders"]:
                for d in app["dar"]["downloaders"]:
                    downloader_id = d["user"]["id"]
                    downloader_id_list.append(downloader_id)

            if app["user"]["id"] == user_id or user_id in downloader_id_list:
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

        """once all the paths that a user could have as user or downloader are gathered and dedupped, make the objects"""
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


        template["users"][user_login] = user_obj ## change this to `template` after testing
    


"""utility functions"""
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
    
    """returns a set of ids and eraLogin for users that have applications, their downloaders, as well as all the applications"""
    users_and_apps = get_users_and_apps()
    
    build_user_permissions(users_and_apps)

    build_yaml(template)
