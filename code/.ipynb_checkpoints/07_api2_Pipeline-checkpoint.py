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

!pip3 install -r requirements.txt

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

# cursor also supports search_filter
# cursor = Cursor(client.list_runtimes,
#                 search_filter = json.dumps({"image_identifier":"jupyter"}))
cursor = Cursor(client.list_runtimes)
runtimes = cursor.items()
for rt in runtimes:
    print(rt.image_identifier)


try:
    # List the available runtime addons, optionally filtered, sorted, and paginated.
    api_response = client.list_runtime_addons(page_size=500)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling CMLServiceApi->list_runtime_addons: %s\n" % e)

cluster = os.getenv("CDSW_DOMAIN")

# Set random hyperparams for jobs

x = random.randint(1, 3)
y = random.randint(1, 4)
z = random.randint(2, 5)

def check_partitions(partitions):
  if partitions > 100:
    partitions = 100
  if partitions < 5:
    partitions = 5
  else:
    return partitions
  return partitions

ROW_COUNT_car_installs = random.randint(10000, 10000)
UNIQUE_VALS_car_installs = random.randint(500, ROW_COUNT_car_installs-1)
PARTITIONS_NUM_car_installs = round(ROW_COUNT_car_installs / UNIQUE_VALS_car_installs)
PARTITIONS_NUM_car_installs = check_partitions(PARTITIONS_NUM_car_installs)

ROW_COUNT_car_sales = random.randint(10000, 10000)
UNIQUE_VALS_car_sales = random.randint(500, ROW_COUNT_car_sales-1)
PARTITIONS_NUM_car_sales = round(ROW_COUNT_car_sales / UNIQUE_VALS_car_sales)
PARTITIONS_NUM_car_sales = check_partitions(PARTITIONS_NUM_car_sales)

ROW_COUNT_customer_data = random.randint(10000, 10000)
UNIQUE_VALS_customer_data = random.randint(500, ROW_COUNT_customer_data-1)
PARTITIONS_NUM_customer_data = round(ROW_COUNT_customer_data / UNIQUE_VALS_customer_data)
PARTITIONS_NUM_customer_data = check_partitions(PARTITIONS_NUM_customer_data)

ROW_COUNT_factory_data = random.randint(10000, 10000)
UNIQUE_VALS_factory_data = random.randint(500, ROW_COUNT_factory_data-1)
PARTITIONS_NUM_factory_data = round(ROW_COUNT_factory_data / UNIQUE_VALS_factory_data)
PARTITIONS_NUM_factory_data = check_partitions(PARTITIONS_NUM_factory_data)

ROW_COUNT_geo_data = random.randint(10000, 100000)
UNIQUE_VALS_geo_data = random.randint(500, ROW_COUNT_geo_data-1)
PARTITIONS_NUM_geo_data = round(ROW_COUNT_geo_data / UNIQUE_VALS_geo_data)
PARTITIONS_NUM_geo_data = check_partitions(PARTITIONS_NUM_geo_data)

print("SPARKGEN PIPELINE SPARK HYPERPARAMS")

print("x: {}", x)
print("y: {}", y)
print("z: {}", z)

print("ROW_COUNT_car_installs: {}", ROW_COUNT_car_installs)
print("UNIQUE_VALS_car_installs: {}", UNIQUE_VALS_car_installs)
print("PARTITIONS_NUM_car_installs: {}", PARTITIONS_NUM_car_installs)

print("ROW_COUNT_car_sales: {}", ROW_COUNT_car_sales)
print("UNIQUE_VALS_car_sales: {}", UNIQUE_VALS_car_sales)
print("PARTITIONS_NUM_car_sales: {}", PARTITIONS_NUM_car_sales)

print("ROW_COUNT_customer_data: {}", ROW_COUNT_customer_data)
print("UNIQUE_VALS_customer_data: {}", UNIQUE_VALS_customer_data)
print("PARTITIONS_NUM_customer_data: {}", PARTITIONS_NUM_customer_data)

print("ROW_COUNT_factory_data: {}", ROW_COUNT_factory_data)
print("UNIQUE_VALS_factory_data: {}", UNIQUE_VALS_factory_data)
print("PARTITIONS_NUM_factory_data: {}", PARTITIONS_NUM_factory_data)

print("ROW_COUNT_geo_data: {}", ROW_COUNT_geo_data)
print("UNIQUE_VALS_geo_data: {}", UNIQUE_VALS_geo_data)
print("PARTITIONS_NUM_geo_data: {}", PARTITIONS_NUM_geo_data)

# Set project ID
project_id = os.environ["CDSW_PROJECT_ID"]

# Create the FIRST JOB
# Create a job. We will create dependent/children jobs of this job, so we call this one a "grandparent job". The parameter "runtime_identifier" is needed if this is running in a runtimes project.
sparkgen_1_job_body = cmlapi.CreateJobRequest(
    project_id = project_id,
    name = "MyJob_1_"+session_id,
    script = "code/cml_job_1.py",
    cpu = 4.0,
    memory = 8.0,
    runtime_identifier = "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-workbench-python3.7-standard:2023.05.1-b4",
    runtime_addon_identifiers = ["spark320-18-hf4"],
    environment = {
                    "x":str(x),
                    "y":str(y),
                    "z":str(z),
                    "ROW_COUNT_car_installs":str(ROW_COUNT_car_installs),
                    "UNIQUE_VALS_car_installs":str(UNIQUE_VALS_car_installs),
                    "PARTITIONS_NUM_car_installs":str(PARTITIONS_NUM_car_installs),
                    "ROW_COUNT_car_sales":str(ROW_COUNT_car_sales),
                    "UNIQUE_VALS_car_sales":str(UNIQUE_VALS_car_sales),
                    "PARTITIONS_NUM_car_sales":str(PARTITIONS_NUM_car_sales),
                    "ROW_COUNT_customer_data":str(ROW_COUNT_customer_data),
                    "UNIQUE_VALS_customer_data":str(UNIQUE_VALS_customer_data),
                    "PARTITIONS_NUM_customer_data":str(PARTITIONS_NUM_customer_data),
                    "ROW_COUNT_factory_data":str(ROW_COUNT_factory_data),
                    "UNIQUE_VALS_factory_data":str(UNIQUE_VALS_factory_data),
                    "PARTITIONS_NUM_factory_data":str(PARTITIONS_NUM_factory_data),
                    "ROW_COUNT_geo_data":str(ROW_COUNT_geo_data),
                    "UNIQUE_VALS_geo_data":str(UNIQUE_VALS_geo_data),
                    "PARTITIONS_NUM_geo_data":str(PARTITIONS_NUM_geo_data)
                    }
)
sparkgen_1_job = client.create_job(sparkgen_1_job_body, project_id)

# Create the SECOND JOB
sparkgen_2_job_body = cmlapi.CreateJobRequest(
    project_id = project_id,
    name = "MyJob_2_"+session_id,
    script = "code/cml_job_2.py",
    cpu = 4.0,
    memory = 8.0,
    runtime_identifier = "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-workbench-python3.7-standard:2023.05.1-b4",
    runtime_addon_identifiers = ["spark320-18-hf4"],
    parent_job_id = sparkgen_1_job.id,
    environment={
                  "x":str(x),
                  "y":str(y),
                  "z":str(z),
                  "ROW_COUNT_car_installs":str(ROW_COUNT_car_installs),
                  "UNIQUE_VALS_car_installs":str(UNIQUE_VALS_car_installs),
                  "PARTITIONS_NUM_car_installs":str(PARTITIONS_NUM_car_installs),
                  "ROW_COUNT_car_sales":str(ROW_COUNT_car_sales),
                  "UNIQUE_VALS_car_sales":str(UNIQUE_VALS_car_sales),
                  "PARTITIONS_NUM_car_sales":str(PARTITIONS_NUM_car_sales),
                  "ROW_COUNT_customer_data":str(ROW_COUNT_customer_data),
                  "UNIQUE_VALS_customer_data":str(UNIQUE_VALS_customer_data),
                  "PARTITIONS_NUM_customer_data":str(PARTITIONS_NUM_customer_data),
                  "ROW_COUNT_factory_data":str(ROW_COUNT_factory_data),
                  "UNIQUE_VALS_factory_data":str(UNIQUE_VALS_factory_data),
                  "PARTITIONS_NUM_factory_data":str(PARTITIONS_NUM_factory_data),
                  "ROW_COUNT_geo_data":str(ROW_COUNT_geo_data),
                  "UNIQUE_VALS_geo_data":str(UNIQUE_VALS_geo_data),
                  "PARTITIONS_NUM_geo_data":str(PARTITIONS_NUM_geo_data),
                  "CML_JOBRUN_timestamp":str(timestamp)
        }
)
sparkgen_2_job = client.create_job(sparkgen_2_job_body, project_id)

# Run the SPARKGEN Jobs
jobrun_body = cmlapi.CreateJobRunRequest(project_id, sparkgen_1_job.id)
job_run = client.create_job_run(jobrun_body, project_id, sparkgen_1_job.id)

print("CML JOB PIPELINE TRIGGERED\n")
print("JOB ID\n")
print(job_run.id)

