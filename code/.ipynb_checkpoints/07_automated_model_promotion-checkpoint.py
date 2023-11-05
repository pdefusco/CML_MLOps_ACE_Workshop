from __future__ import print_function
import time
import cmlapi
from cmlapi.rest import ApiException
from pprint import pprint
import os
import json

#api_instance = cmlapi.CMLServiceApi()

client = cmlapi.default_client()

search_filter = {"creator.username":"pauldefusco", "name":"mlflow_new"}
search = json.dumps(search_filter)
client.list_projects()

project_id = os.environ['CDSW_PROJECT_ID']


# List Registered Models at Workspace Level

try:
    # List registered models.
    api_response = client.list_registered_models()
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->list_registered_models: %s\n" % e)

    
# List Registered Models at Workspace Level filter by username:

try:
    # List registered models.
    search_filter = {"creator_id":"vishrajagopalan"}
    search = json.dumps(search_filter)
    page_size = 100
    api_response = client.list_registered_models(search_filter=search, page_size=page_size)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->list_registered_models: %s\n" % e)
    
# List ALL Experiments

try:
    # Lists all experiments that belong to a user across all projects.
    search_filter = {"creator.name":"Paul de Fusco"}
    search = json.dumps(search_filter)
    api_response = client.list_all_experiments(search_filter=search)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->list_all_experiments: %s\n" % e)

# List Projects    

try:
    # Return all projects, optionally filtered, sorted, and paginated.
    search_filter = {"owner.name" : "Paul de Fusco"}
    search = json.dumps(search_filter)
    api_response = client.list_projects(search_filter=search)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->list_projects: %s\n" % e)

# List Experiments
    
try:
    # List all experiments in a given project.
    api_response = client.list_experiments(project_id = os.environ["CDSW_PROJECT_ID"])
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->list_experiments: %s\n" % e)

experiment_id = 'opct-qzll-cyeb-gj9v' # str | Experiment ID to search over.
#search_filter = 'search_filter_example' # str | Search filter is an optional HTTP parameter to filter results by. Supported search filter keys are: [creator.email creator.name creator.username name status]. Dynamic search key words are supported for experiment runs. Supported fields are [metrics tags params]. (optional)
page_size = 40 # int | Page size is an optional argument for number of entries to return in one page. If not specified, the server will determine a page size. If specified, must be respecified for further requests when using the provided next page token in the response. (optional)

try:
    # Returns a list of Runs that belong to an experiment.
    api_response = client.list_experiment_runs(project_id, experiment_id)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->list_experiment_runs: %s\n" % e)

    
CreateRegisteredModelRequest = {"project_id": project_id, 
                                "experiment_id" : "opct-qzll-cyeb-gj9v", 
                                "run_id": "y1o3-gtvz-tcrm-oxht", 
                                "model_name": "api_model_new", 
                                "model_path": "artifacts"
                               }

#body = cmlapi.CreateRegisteredModelRequest() # CreateRegisteredModelRequest | 

try:
    # Register a model.
    api_response = client.create_registered_model(CreateRegisteredModelRequest)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->create_registered_model: %s\n" % e)


