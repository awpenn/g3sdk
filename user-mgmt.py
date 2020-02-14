from dataload_functions import *
import json
import yaml
from requests.auth import AuthBase
import requests
from datetime import datetime

from settings import APIURL, HEADERS

def get_datasets():
    response = requests.get(APIURL+"datasets", headers=HEADERS)
    dataset_data = response.json()['data']

    datasets_and_consents = []

    for dataset in dataset_data:
        accession_no = dataset["accession"]

        dss_dataset_id = dataset["activeVersion"]["id"]
  
        version_consents = getConsents(dss_dataset_id)

        datasets_and_consents.append( (accession_no, version_consents) )

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
    for app in all_applications:
        expiration_date_unformatted = app["expiration_date"].replace("T", " ")
        expiration_date = datetime.strptime(expiration_date_unformatted[ :expiration_date_unformatted.index('.')], '%Y-%m-%d %H:%M:%S') 
       
        if app["active"] and not app["dar"]["close_out_status"] and expiration_date > current_time:
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

                        if app["dar"]["downloaders"]:
                            for d in app["dar"]["downloaders"]:
                                downloader_id = d["user"]["id"]
                                downloader_login = d["user"]["user_login"]

                                downloader_tup = (downloader_id, downloader_login)
                                active_users.add(downloader_tup)
    
    return [list(active_users), all_applications]

def build_user_permissions(users_and_apps):
    project_checklist = build_consent_level_checklist()
    
    system_resource_dict = create_system_checkstrings(project_checklist)

    users = users_and_apps[0]
    apps = users_and_apps[1]

    for user in users:
        user_id = user[0]
        user_login = user[1]

        """ultimately added to template as `template["users"][user_login] = user_obj , where user_login is the key (template["users"] is a dict)"""
        user_obj = {
            "admin": False,
            "projects": []
        }
        
        """removes duplicate permissions across applications"""
        resource_set = set()
        programs_granted_all_consent = set()
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

                """actually creating the permissions"""
                for resource_tuple in dataset_and_consent:
                    program = resource_tuple[0]
                    project = program + "_" + resource_tuple[1]

                    if project in project_checklist:
                        resource_path = "/programs/" + program + "/projects/" + project
                        resource_set.add(resource_path)

        """once all the paths that a user could have as user or downloader are gathered and dedupped, make the objects"""

        user_resource_dict = create_user_checkstrings(resource_set)
  
        ## if strings match, only add "ALL", else do for resource_path in resource_set below


        for resource_path in resource_set:
            program = parse_resource_path(resource_path)[0]
            auth_id = None

            grant_all_consent = check_program_for_allcon(resource_path, system_resource_dict, user_resource_dict)
            
            """adds ALL consent level if the check returns true, but still adds all the others"""
            if grant_all_consent and program not in programs_granted_all_consent:
                auth_id = program+ "_" + "ALL"
                res_path = "/programs/" + program + "/projects/" + auth_id
                programs_granted_all_consent.add(program)

                project_obj = {
                    "auth_id": auth_id,
                    "privilege": [
                        "read"
                    ],
                    "resource": res_path
                }

                programs_granted_all_consent.add(program)
                user_obj["projects"].append(project_obj)

            res_path = resource_path
            delim = "/projects/"
            delimlen = len(delim)
            slindex = resource_path.index(delim)
            auth_id = resource_path[ slindex + delimlen:]

            project_obj = {
                "auth_id": auth_id,
                "privilege": [
                    "read"
                ],
                "resource": res_path
            }

            user_obj["projects"].append(project_obj)

            if not grant_all_consent:
                files_project_obj = {
                "auth_id": auth_id + "_files",
                "privilege": [
                    "read"
                ],
                "resource": res_path + "_files"
                }

                user_obj["projects"].append(files_project_obj)

        template["users"][user_login] = user_obj

def add_ALL_to_resource_tree():

    for program in template["rbac"]["resources"][0]["subresources"]: ##change to use real template once built
        project_name = program["name"] + "_" + "ALL"
        program["subresources"][0]["subresources"].append( {"name": project_name } )

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
    user_yaml = yaml.dump(yaml.load(json.dumps(template)), default_flow_style=False)
    
    """for now 2/4 just going to write final product here"""
    with open("jsondumps/user.yaml", "w") as outfile:
        outfile.write(user_yaml)

def build_consent_level_checklist():
    resources = template["rbac"]["resources"][0]["subresources"]
    checklist = []
    for program in resources:
        for project in program["subresources"][0]["subresources"]:
            checklist.append(project["name"])
    
    return checklist

def parse_resource_path(resource_path):
    delim = "/projects/"
    delimlen = len(delim)
    slindex = resource_path.index(delim)
    consent = resource_path[ slindex + delimlen:]

    prog_delimlen = len("/programs/")
    program = resource_path[ prog_delimlen:slindex ]
        
    return [program, consent]
        
def create_user_checkstrings(resource_set):

    """takes a set of resource_paths created from app loop"""
    user_resource_dict = {}

    for resource_path in resource_set:
        parsed_program_consent = parse_resource_path(resource_path)

        program = parsed_program_consent[0]

        if program not in user_resource_dict:
            user_resource_dict[program] = ''
    

    for key in user_resource_dict:
        program_sortset = set()
        program_sortstring = ''
        for resource in resource_set:
            parsed_program_consent = parse_resource_path(resource)

            program = parsed_program_consent[0]
            consent = parsed_program_consent[1]
                        
            if program == key:
                program_sortset.add(consent)
                    
        for project in sorted(list(program_sortset)):

            if program_sortstring:
                program_sortstring += project
            else:
                program_sortstring = project
                    
        user_resource_dict[key] = program_sortstring
    
    return user_resource_dict

def create_system_checkstrings(project_checklist):
    system_resource_dict = {}

    def parse_resource(resource):
        slindex = resource.index("_")
        program = resource[:slindex]
        project = resource[slindex + 1:]

        return [program, project]

    for resource in project_checklist:
        program = parse_resource(resource)[0]

        if program not in system_resource_dict:
            system_resource_dict[program] = ''
    
    for key in system_resource_dict:
        program_sortset = set()
        program_sortstring = ''

        for resource in project_checklist:
            parsed_program_consent = parse_resource(resource)

            program = parsed_program_consent[0]
            consent = parsed_program_consent[1]

            project = program + "_" + consent

            if program == key:
                program_sortset.add(project)

        for project in sorted(list(program_sortset)):

            if program_sortstring:
                program_sortstring += project
            else:
                program_sortstring = project
                    
        system_resource_dict[key] = program_sortstring
    
    return system_resource_dict

def check_program_for_allcon(resource, system_resource_dict, user_resource_dict):
    program = parse_resource_path(resource)[0]

    if system_resource_dict[program] == user_resource_dict[program]:
        return True
    else:
        return False




if __name__ == "__main__":
    current_time = datetime.now()
    template = open_template()

    datasets = get_datasets()

    build_resource_descriptions(datasets)
    
    """returns a set of ids and eraLogin for users that have applications, their downloaders, as well as all the applications"""
    users_and_apps = get_users_and_apps()

    build_user_permissions(users_and_apps)

    add_ALL_to_resource_tree()

    build_yaml(template)
