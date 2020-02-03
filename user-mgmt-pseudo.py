from dataload_functions import *
import pandas as pd
import json
from requests.auth import AuthBase
import requests

from settings import APIURL, HEADERS

"""stubbing out how user information will be gatherd to build user.yaml file for gen3 user management"""

"""step one is define all the resources in datastage"""
def get_datasets():
    """use method in loadscript to get list of datasets, ultimately resulting in...
        dataset_data = response.json()
    """
    datasets_and_consents = []

    for dataset in dataset_data:
        accession_no = dataset["accession"]

        for version in dataset["datasetVersions"]:
            if version["active"] == 1:
                dss_dataset_id = version["id"]
        
        version_consents = getConsents(dss_dataset_id)

        datasets_and_consents.append( [accession_no, version_consents] )

    return [datasets_and_consents]

"""this function will take the nested list returned from get_datasets and use it to build the json or yaml blocks that make up the resources section of the user.yaml file"""
def build_resource_descriptions(dc): """dc = calling the get_datasets function"""
    for dataset in dc:
        program_name = dataset[0]
        projects = dataset[1]

        """make a resource object in the resource section along the lines of `- name: program_name`"""
        if len(projects) > 0:
            """add `subresources` block"""
            for project in projects:
                project_name = program_name + "_" + project
                """add to subresources `-{name: project_name}"""




"""user info section"""
"""currently, api doesn't support getting a list of users, and if we have users with no applications (?) then no need to add them anyway"""


def get_users_and_apps():
    subroute = 'api/applications?per_page=500'       """aw - will have to change to paginated collection as elsewhere but fine for now"""
    requrl = APIURL + subroute
    response = requests.get(requrl, headers=HEADERS)
    all_applications = response.json()['data']

    active_users = set()
    for app in all_applications:
        user = app['user']['id']
        email = app['user']['email']

        user_tup = (user, email)

        active_users.add(user_tup)
    
    return [active_users, all_applications]

"""returns a set of ids and emails (may need to be changed to eRA id or whatever in the future) for users that have applications, as well as all the applications"""

users_and_apps = get_users_and_apps()

def build_user_permissions(users_and_apps):
    users = users_and_apps[0]
    apps = users_and_apps[1]

    for user in active_users:
        email = user[0]
        user_id = user[1]
        """build a block under `users` section with email, then"""

        for app in users_and_apps:
            if app["user"]["id"] == user_id:

                subroute = 'api/applications/' + str(app["id"]) + '/approvedConsents'     
                requrl = APIURL + subroute
                response = requests.get(requrl, headers=HEADERS)
                approved_consents = response.json()['data']
                
                dataset_and_consent = [] """will be a list of tuples = dataset and consent to the iterate through for resources"""
                for approved_consent in approved_consents:
                    consent = approved_consent["consent"]["key"]
                    dataset_accession = approved_consent["dataset"]["accession"]
                    dataset_and_consent.append( (dataset_accession, consent) )
                    
                for resource_tuple in dataset_and_consent:
                    program = resource_tuple[0]
                    project = program + "_" + resource_tuple[1]

                    """build block under users along the lines of:
                    - auth_id: [project]
                      privilege: ['read']
                      resource: /programs/[program]/projects/[project]
                    """