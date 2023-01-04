# ###########################################################################
#
#  CLOUDERA APPLIED MACHINE LEARNING PROTOTYPE (AMP)
#  (C) Cloudera, Inc. 2021
#  All rights reserved.
#
#  Applicable Open Source License: Apache 2.0
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
# ###########################################################################


!pip3 install -r requirements.txt

import os
import cmlapi
from cmlapi.utils import Cursor
import string
import random
import json

cluster = os.getenv("CDSW_DOMAIN")

try:
    client = cmlapi.default_client()
except ValueError:
    print("Could not create a client. If this code is not being run in a CML session, please include the keyword arguments \"url\" and \"cml_api_key\".")

session_id = "".join([random.choice(string.ascii_lowercase) for _ in range(6)])
session_id

# List projects using the default sort and default page size (10)
client.list_projects(page_size = 20)

project_id = os.environ["CDSW_PROJECT_ID"]

### CREATE AN ENDPOINT AND PUSH THE TF MODEL ###

#Would be nice to name it with job id rather than session id
modelReq = cmlapi.CreateModelRequest(
    name = "demo-model-" + session_id,
    description = "model created for demo",
    project_id = project_id,
    disable_authentication = True
)

model = client.create_model(modelReq, project_id)

model_build_request = cmlapi.CreateModelBuildRequest(
    project_id = project_id,
    model_id = model.id,
    comment = "test comment",
    file_path = "01_ML_Project_Basics/6_B_CustomerEndpoint.py",
    function_name = "predict",
    kernel = "python3",
    runtime_identifier = "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-workbench-python3.9-standard:2022.11.1-b2"
    #runtime_addon_identifiers = "spark311-13-hf1"
)

modelBuild = client.create_model_build(
    model_build_request, project_id, model.id
)

model_deployment = cmlapi.CreateModelDeploymentRequest(
        project_id = project_id, 
        model_id = model.id, 
        build_id = modelBuild.id, 
        cpu = 1.00,
        memory = 2.00
    )

model_deployment_response = client.create_model_deployment(
        model_deployment, 
        project_id = project_id, 
        model_id = model.id, 
        build_id = modelBuild.id
    )
