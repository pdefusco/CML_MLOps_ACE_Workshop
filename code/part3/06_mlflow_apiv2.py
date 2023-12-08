#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

from __future__ import print_function
import cmlapi
from cmlapi.rest import ApiException
from pprint import pprint
import json, secrets, os, time
from datetime import datetime
import random


client = cmlapi.default_client()

client.list_projects()

project_id = os.environ['CDSW_PROJECT_ID']
username = os.environ["PROJECT_OWNER"]

## DEPLOY A MODEL THAT HAS ALREADY BEEN REGISTERED VIA MLFLOW LOGGING ROUTINE

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
    search_filter = {"creator_id":"pauldefusco"}
    search = json.dumps(search_filter)
    page_size = 100
    api_response = client.list_registered_models(search_filter=search, page_size=page_size)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->list_registered_models: %s\n" % e)
    
# List ALL Experiments

try:
    # Lists all experiments that belong to a user across all projects.
    search_filter = {"name":"xgboost-clf-{}".format(username)}
    search = json.dumps(search_filter)
    api_response = client.list_all_experiments(search_filter=search)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->list_all_experiments: %s\n" % e)

try:
    # Lists all experiments that belong to a user across all projects.
    search_filter = {"name":"pytorch-clf-{}".format(username)}
    search = json.dumps(search_filter)
    api_response = client.list_all_experiments(search_filter=search)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->list_all_experiments: %s\n" % e)
    

# List All Projects owned by You:

try:
    # Return all projects, optionally filtered, sorted, and paginated.
    search_filter = {"owner.username" : username}
    search = json.dumps(search_filter)
    api_response = client.list_projects(search_filter=search)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->list_projects: %s\n" % e)

# List All Experiments
  
try:
    # List all experiments in a given project.
    api_response = client.list_experiments(project_id = os.environ["CDSW_PROJECT_ID"])
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->list_experiments: %s\n" % e)
    
experiment_id = api_response.to_dict()["experiments"][0]['id'] # str | Experiment ID to search over.
#search_filter = 'search_filter_example' # str | Search filter is an optional HTTP parameter to filter results by. Supported search filter keys are: [creator.email creator.name creator.username name status]. Dynamic search key words are supported for experiment runs. Supported fields are [metrics tags params]. (optional)
page_size = 40 # int | Page size is an optional argument for number of entries to return in one page. If not specified, the server will determine a page size. If specified, must be respecified for further requests when using the provided next page token in the response. (optional)

try:
    # Returns a list of Runs that belong to an experiment.
    api_response = client.list_experiment_runs(project_id, experiment_id)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->list_experiment_runs: %s\n" % e)

## Register a Model from an Experiment
## This provides an Entry in the MLFlow Model Registry
# Retrieve the experiment Run ID from the output of the previous command and enter it below:

api_response.experiment_runs[0].id

session_id = secrets.token_hex(nbytes=4)
run_id = api_response.experiment_runs[0].id
model_name = 'wine_model_' + username + "-" + session_id

CreateRegisteredModelRequest = {
                                "project_id": project_id, 
                                "experiment_id" : experiment_id,
                                "run_id": run_id, 
                                "model_name": model_name, 
                                "model_path": "artifacts"
                               }

try:
    # Register a model.
    api_response = client.create_registered_model(CreateRegisteredModelRequest)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->create_registered_model: %s\n" % e)
    
## LIST REGISTERED MODELS

# Allow for model registration to complete before continuing

sort = 'created_at' # str | Sort is an optional HTTP parameter to sort results by. Supported sort keys are: [created_at creator.email creator.name creator.username description kernel name script status updated_at]. where \"+\" means sort by ascending order, and \"-\" means sort by descending order. For example:   sort=-created_at. (optional)

try:
    # List registered models.
    api_response = client.list_registered_models(sort=sort)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->list_registered_models: %s\n" % e)

model_id = api_response.models[-1].model_id
      
  
# current date and time
now = datetime.now()
timestamp = datetime.timestamp(now)

##### CREATE DEV PROJ

createProjRequest = {"name": "mlops_dev_prj", "template":"git", "git_url":"https://github.com/pdefusco/MLOps_CML_DEV_Proj.git"}

try:
    # Create a new project
    api_response = client.create_project(createProjRequest)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->create_project: %s\n" % e)  
  
  