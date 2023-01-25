# ###########################################################################
#
#  CLOUDERA APPLIED MACHINE LEARNING PROTOTYPE (AMP)
#  (C) Cloudera, Inc. 2023
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

import os, json, string, time
import cmlapi
from cmlapi.rest import ApiException
from pprint import pprint
import random
import logging
import yaml
from yaml.loader import SafeLoader
from packaging import version

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

log_file = "logs/simulation.log"
os.makedirs(os.path.dirname(log_file), exist_ok=True)
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(file_handler)

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


    def get_job(self, jobId):
        """
        Get Job based on Job ID.
        The representation can be used to easily reproduce project artifacts in other environments.
        Returns a response with an instance of type Job.
        """
        try:
            # Return one job.
            jobResponse = self.client.get_job(self.project_id, jobId, async_req=True).get().to_dict()
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


    def list_job_runs(self, jobId):
        """
        List all Project Job Runs.
        The representation can be used to easily reproduce job-related project artifacts in other environments.
        Returns a response with an instance of type ListJobRunsResponse.
        """
        try:
            # Lists job runs, optionally filtered, sorted, and paginated.
            listJobRunsResponse = self.client.list_job_runs(self.project_id, jobId, async_req=True).get().to_dict()
            pprint(listJobRunsResponse)
        except ApiException as e:
            print("Exception when calling CMLServiceApi->list_job_runs: %s\n" % e)

        return listJobRunsResponse


    def list_latest_model_build(self):
        """
        List Most Recent Model Build for each Project Model.
        CML Best Practices dictates having one Production Model per Project, and managing multiple Builds and Deployments within it.
        The method thus retrieves configurations related to the latest model build for the project model. 
        The representation can be used to easily reproduce model-related project artifacts in other environments.
        Returns a response with an instance of type ListModelsResponse.
        """
        # gather model details
        models = (
            self.client.list_models(project_id=self.project_id, async_req=True, page_size = 50)
            .get()
            .to_dict()
        )
        model_info = [
            model for model in models["models"]
        ][-1]

        modelId = model_info["id"]

        # gather latest build details
        builds = (
            self.client.list_model_builds(
                project_id=self.project_id, model_id=modelId, async_req=True, page_size = 50
            )
            .get()
            .to_dict()
        )
        listModelBuildsResponse = builds["model_builds"][-1]  # most recent build

        return listModelBuildsResponse


    def create_job_body_from_scratch(self, job_name, script, cpu, mem, parent_job, runtimeId, *runtime_addon_ids):
        """
        Create a Job Request Body via APIv2 given an APIv2 client object and Job Details.
        This function only works for models deployed within the current project.
        """
        job_body = cmlapi.CreateJobRequest(
            project_id = self.project_id,
            name = job_name,
            script = script,
            cpu = cpu,
            memory = mem,
            runtime_identifier = runtimeId,
            runtime_addon_identifiers = list(runtime_addon_ids)
        )

        print("Job Body for Job {}: ".format(jobBody.name))
        print(jobBody)

        return jobBody


    def create_job_body_from_jobresponse(self, jobResponse):
        """
        Create a Job Body with an instance of Job type as input.
        This function helps you reproduce a Job from one Project to Another.
        """
        job_body = cmlapi.CreateJobRequest(
            project_id = self.project_id,
            name = jobResponse["name"],
            script = jobResponse["script"],
            cpu = jobResponse["cpu"],
            memory = jobResponse["memory"],
            runtime_identifier = jobResponse["runtime_identifier"],
            runtime_addon_identifiers = jobResponse["runtime_addon_identifiers"]
        )
        print("Job Body for Job {}: ".format(jobBody.name))
        print(jobBody)

        return jobBody

      
    def create_model_body_from_modelresponse(self, modelResponse):
        """
        Create a Job Body with an instance of Job type as input.
        This function helps you reproduce a Job from one Project to Another.
        """
        modelBody = cmlapi.CreateJobRequest(
            project_id = self.project_id,
            crn = modelResponse["crn"],
            file_path = modelResponse["file_path"],
            updated_at = modelResponse["updated_at"],
            function_name = modelResponse["function_name"],
            runtime_identifier = modelResponse["runtime_identifier"],
            runtime_addon_identifiers = modelResponse["runtime_addon_identifiers"]
            creator_name = modelResponse["creator"]["name"]
        )
        print("Model Body for Model CRN: {}: ".format(crn))
        print(modelBody)

        return modelBody
      

    def create_job(self, jobBody):
        """
        Create a Job via APIv2 given an APIv2 client object and Job Body.
        This function only works for models deployed within the current project.
        """
        job_instance = client.create_job(jobBody, self.project_id)
        print("Job with Name {} Created Successfully".format(job_body.name))

        return jobInstance


    def run_job(self, jobBody, jobId):
        """
        Run a Job via APIv2 given an APIv2 client object, Job Body and Job Create Instance.
        This function only works for models deployed within the current project.
        """
        job_run = self.client.create_job_run(job_body, self.project_id, job_id)
        print("Job {0} Run with Run ID {1}".format(job_body.name, job_run.id))

        return jobRun


    def update_project_metadata(self, yaml_dict):
        """
        Create empty project-metadata.yaml file for the first time
        """

        sdump = yaml.dump(
                    yaml_dict
                    ,indent=4
                    )

        with open("project-metadata.yaml", "a") as fo:
            fo.write(sdump)

        with open("project-metadata.yaml", "a") as fo:
            fo.write("\n")


    def remove_project_metadata(self, base_path='/home/cdsw', file_name='project-metadata.yaml'):
        """
        Remove current project-metadata.yaml file for the first time
        Assumes the file is named project-metadata.yaml and the path is /home/cdsw/
        """
        print("Directory Contents before File Removal")
        print(os.listdir(base_path))
        
        try:
          os.remove(base_path + "/" + file_name)
        except:
          print("File not found")
        
        print("Directory Contents after File Removal")
        print(os.listdir(base_path))


    def read_proj_metadata(self, yaml_file):
        """
        Parse project metadata yaml so project artifacts can be reproduced in same or new project.
        """
        with open(yaml_file, 'r') as f:
            data = list(yaml.safe_load_all(f))#, Loader=yaml.Loader)
            all_metadata = list(data[0].items())
            print(all_metadata)

        return all_metadata


    def create_job_yaml(self, jobResponse):
        """
        Create YAML snippet for a single job based on jobResponse instance
        """
        jobId = 'Job_' + jobResponse['id']
        job_yaml_dict = {
          jobId: {
              'jobResponse': jobResponse,
              'requirements': '/home/cdsw/requirements.txt',
              'last_updated_timestamp': time.time() * 1000
            }
        }

        return job_yaml_dict


    def create_model_yaml(self, modelResponse):
        """
        Create YAML snippet for a single model based on modelResponse instance
        """
        modelId = 'Model_' + modelResponse['model_id']
        model_yaml_dict = {
          modelId: {
              'modelResponse': modelResponse,
              'requirements': '/home/cdsw/requirements.txt',
              'last_updated_timestamp': time.time() * 1000
            }
        }

        return model_yaml_dict

      
    def reproduce_project_from_metadata(self, proj_metadata):
        """
        Use project metadata to create jobs and models.
        """ 
        for response in proj_metadata:
          
            if "Model" in response[0]:
                modelBody = self.create_model_body_from_modelresponse(response[1])
                self.create_model(modelBody)
          
            if "Job" in response[0]:
                jobBoday = self.create_job_body_from_jobresponse(response[1])
                self.create_job(jobBody)
        
        print("Project Deployment Complete. \n")
        print("Listing All Current Project Jobs: \n")
        self.list_jobs()
        print("Listing All Current Project Models: \n")
        self.client.list_models()
        
        
    def create_model_request(self):
        """
        Create a New CML Model Endpoint.
        This function only works for models deployed within the current project.
        """

        rand_id = "".join([random.choice(string.ascii_lowercase) for _ in range(6)])
        modelReq = cmlapi.CreateModelRequest(
            name = "demo-model-" + rand_id,
            description = "model created for demo",
            project_id = self.project_id,
            disable_authentication = True
        )

        return modelReq

      
    def create_model(self, modelBody):
        try:
          # Create a model.
            api_response = api_instance.create_model(body, project_id)
            pprint(api_response)
        except ApiException as e:
            print("Exception when calling CMLServiceApi->create_model: %s\n" % e)



    def create_model_endpoint(self, modelReq):
        """
        Create a New CML Model Endpoint.
        This function only works for models deployed within the current project.
        """

        model = self.client.create_model(modelReq, self.project_id)

        return model


    def create_model_build_request(self, modelId, runtimeId):
        """
        Create a New CML Model Build Request.
        Requires a Model Endpoint.
        This function only works for models deployed within the current project.
        """

        model_build_request = cmlapi.CreateModelBuildRequest(
            project_id = project_id,
            model_id = modelId,
            comment = "test comment",
            file_path = self.base_model_file_path,
            function_name = self.function_name,
            kernel = "python3",
            runtime_identifier = runtimeId
        )

        return modelBuildRequest


    def create_model_build(self, modelId, model_build_request):
        """
        Create a New CML Model Build.
        Requires a Model Build Request object and a Model Endpoint object.
        This function only works for models deployed within the current project.
        """

        modelBuild = client.create_model_build(
            model_build_request, self.project_id, modelId
        )

        return modelBuild


    def create_model_deployment_request(self, modelId, modelBuildId, cpu, mem):
        """
        Create a New CML Model Deployment Request.
        Requires a Model Build object and a Model Endpoint object.
        This function only works for models deployed within the current project.
        """

        model_deployment = cmlapi.CreateModelDeploymentRequest(
            project_id = self.project_id,
            model_id = modelId,
            build_id = modelBuildId,
            cpu = cpu,
            memory = mem
        )

        return modelDeploymentRequest


    def create_model_deployment(self, modelDeploymentRequest, modelId, buildId):
        """
        Create a New CML Model Deployment.
        Requires a Model Deployment Request, a Model Build object and a Model Endpoint object.
        This function only works for models deployed within the current project.
        """

        model_deployment_response = client.create_model_deployment(
                modelDeploymentRequest,
                project_id = self.project_id,
                model_id = modelId,
                build_id = buildId
            )

        return modelDeploymentResponse


    def create_backup_dir(self, backup_path, base_model_script_path, base_model_training_data_path, loaded_model_clf):
        """
        Create a new Dir in the CML Project folders in order to back up model deployment artifacts.
        Creates a copy of the model script, the training data, and the model estimator.
        """
        os.mkdir(backup_path)
        shutil.copyfile(base_model_script_path, backup_path)
        shutil.copyfile(base_model_training_data_path, backup_path)
        modelManager.store_latest_model_version(loaded_model_clf, backup_path)
        print("Model Artifacts Backed Up to path: {}", backup_path)
        print(os.listdir(backup_path))

        return os.listdir(backup_path)

    #def get_all_model_details(self):
    #    """
    #    Create a metadata representation of all jobs and related artifacts as of time of execution.
    #    The representation can be used to easily reproduce project artifacts in other environments.
    #    """
