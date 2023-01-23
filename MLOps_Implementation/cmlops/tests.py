import os, time, json, string
import cmlapi
from cmlapi.rest import ApiException
from pprint import pprint
import random
import logging
from packaging import version
from MLOps_Implementation.cmlops import project_manager

manager = project_manager.CMLProjectManager()
listJobsResponse = manager.list_jobs()
jobResponse = listJobsResponse['jobs'][1]

jobBodyYaml = manager.create_yaml_job(jobResponse)

manager.update_project_metadata(jobBodyYaml)

proj_metadata = manager.read_proj_metadata('/home/cdsw/project-metadata.yaml')

jobBody = manager.create_job_body_from_jobresponse(proj_metadata[0])
