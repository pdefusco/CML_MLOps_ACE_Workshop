import os
import json
import string
import cmlapi
from cmlapi.rest import ApiException
from pprint import pprint
import random
import logging
from packaging import version

from MLOps_Implementation.cmlops import project_manager

manager = project_manager.CMLProjectManager()
listJobsResponse = manager.list_jobs()
jobResponse = listJobsResponse['jobs'][0]
jobBody = manager.create_job_body_from_jobresponse(jobResponse)

jobBodyYaml = manager.create_yaml_job(jobBody)

manager.update_project_metadata(jobBodyYaml)