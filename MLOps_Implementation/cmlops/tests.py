import os
import json
import string
import cmlapi
from cmlapi.rest import ApiException
from pprint import pprint
import random
import logging
from packaging import version

#from MLOps_Implementation.cmlops.project_manager import *


#projManager = CMLProjectManager()

#listJobsResponse = projManager.list_jobs().get().to_dict()
#projManager.get_job()
#.get().to_dict()

#async_req=True


class CMLProjectManager:

    """A class for managing CML Project resources with CML API_v2
    This class contains methods that wrap API_v2 to
    facilitate the first-time creation or redeployment of CML Project models and jobs
    Attributes:
        client (cmlapi.api.cml_service_api.CMLServiceApi)
    """

    def __init__(self):
      self.project_id = os.environ["CDSW_PROJECT_ID"]
      self.cml_workspace_url = os.environ["CDSW_DOMAIN"]
      self.client = cmlapi.default_client()

    def get_job(self, job_id):
      """
      Get Job based on Job ID.
      The representation can be used to easily reproduce project artifacts in other environments.
      Returns a response with an instance of type Job.
      """
      try:
          # Return one job.
          jobResponse = self.client.get_job(project_id, job_id, async_req=True)
          pprint(jobResponse)
      except ApiException as e:
          print("Exception when calling CMLServiceApi->get_job: %s\n" % e)

      return jobResponse
      
    def list_jobs(self):
      """
      List all Project Jobs.
      The representation can be used to easily reproduce project artifacts in other environments.
      Returns a response with an instance of type ListJobsResponse.
      """
      try:
          # Returns all jobs, optionally filtered, sorted, and paginated.
          listJobsResponse = self.client.list_jobs(self.project_id, async_req=True).get().to_dict()
          pprint(listJobsResponse)
      except ApiException as e:
          print("Exception when calling CMLServiceApi->list_jobs: %s\n" % e)

      return listJobsResponse