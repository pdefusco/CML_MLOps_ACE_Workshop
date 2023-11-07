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

import os
from cmlapi.utils import Cursor
import string
import random
import json
from __future__ import print_function
import cmlapi
from cmlapi.rest import ApiException
from pprint import pprint
from datetime import datetime
import time

# current date and time
now = datetime.now()
timestamp = datetime.timestamp(now)

try:
    client = cmlapi.default_client()
except ValueError:
    print("Could not create a client. If this code is not being run in a CML session, please include the keyword arguments \"url\" and \"cml_api_key\".")

session_id = "".join([random.choice(string.ascii_lowercase) for _ in range(6)])
session_id

##### CREATE TEAM FOR DEV PROJ

"""createTeamRequest = {type	str		[optional]
cn	str	cn of the team.	[optional]
bio	str	bio of the team.	[optional]}"""

body = {"username":"pauldefusco", "name" : "A_TEAM"}

try:
    # Create a team.
    api_response = client.create_team(body)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->create_team: %s\n" % e)

##### CREATE DEV PROJ

createProjRequest = {"name": "mlops_dev_prj", "template":"git", "git_url":"https://github.com/pdefusco/MLOps_CML_DEV_Proj.git"}

try:
    # Create a new project
    api_response = client.create_project(createProjRequest)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->create_project: %s\n" % e)
    
    
"""
name	str	The name of the project to create.	[optional]
description	str	The description of the project.	[optional]
visibility	str	The visibility of the project (one of \"public\", \"organization\", \"private\"). Default is private.	[optional]
parent_project	str	Optional parent project to fork.	[optional]
git_url	str	Optional git URL to checkout for this project.	[optional]
template	str	Optional template to use (Python, R, PySpark, Scala, Churn Predictor) Note: local will create the project but nothing else, files must be uploaded separately.	[optional]
organization_permission	str	If this is an organization-wide project, the visibility to others in the organization.	[optional]
default_project_engine_type	str	Whether this project uses legacy engines or runtimes. Valid values are \"ml_runtime\", \"legacy_engine\", or leave blank to default to the site-wide default.	[optional]
environment	dict(str, str)		[optional]
shared_memory_limit	int	Additional shared memory limit that engines in this project should have, in MB (default 64).	[optional]
team_name	str		[optional]
"""