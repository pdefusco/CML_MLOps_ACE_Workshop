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

import os
import json
import string
import cmlapi
import random
import logging
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

class CMLProductionModel:
    """A class for managing CML Production Deployments with CML API_v2
    This class contains methods that wrap API_v2 to achieve specific
    needs that facilitate the creation and productionization of a Model Pipeline.
    Attributes:
        client (cmlapi.api.cml_service_api.CMLServiceApi)
    """

    def __init__(self, base_model_file_path, base_model_script_path, base_model_training_data_path, project_id, function_name):
        self.client = cmlapi.default_client()
        self.base_model_file_path = base_model_file_path
        self.base_model_script_path = base_model_script_path
        self.base_model_training_data_path = base_model_training_data_path
        self.project_id = project_id
        #self.model_name = model_name
        self.function_name = function_name


    def get_latest_deployment_details(self, model_name):
        """
        Given a APIv2 client object and Model Name, use APIv2 to retrieve details about the latest/current deployment.
        This function only works for models deployed within the current project.
        """

        # gather model details
        models = (
            self.client.list_models(project_id=self.project_id, async_req=True)
            .get()
            .to_dict()
        )
        model_info = [
            model for model in models["models"] if model["name"] == model_name
        ][0]

        model_id = model_info["id"]
        model_crn = model_info["crn"]
        model_access_key = model_info["access_key"]

        # gather latest build details
        builds = (
            self.client.list_model_builds(
                project_id=project_id, model_id=model_id, async_req=True
            )
            .get()
            .to_dict()
        )
        build_info = builds["model_builds"][-1]  # most recent build

        build_id = build_info["id"]

        # gather latest deployment details
        deployments = (
            self.client.list_model_deployments(
                project_id=project_id,
                model_id=model_id,
                build_id=build_id,
                async_req=True,
            )
            .get()
            .to_dict()
        )
        deployment_info = deployments["model_deployments"][-1]  # most recent deployment

        model_deployment_crn = deployment_info["crn"]

        return {
            "model_name": model_name,
            "model_id": model_id,
            "model_crn": model_crn,
            "model_access_key": model_access_key,
            "latest_build_id": build_id,
            "latest_deployment_crn": model_deployment_crn,
        }

    def get_latest_deployment_details_allmodels(self):
        """
        Given a APIv2 client object and Model Name, use APIv2 to retrieve details about the latest/current deployment.
        This function only works for models deployed within the current project.
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

        model_id = model_info["id"]
        model_crn = model_info["crn"]
        model_access_key = model_info["access_key"]

        # gather latest build details
        builds = (
            self.client.list_model_builds(
                project_id=project_id, model_id=model_id, async_req=True, page_size = 50
            )
            .get()
            .to_dict()
        )
        build_info = builds["model_builds"][-1]  # most recent build

        build_id = build_info["id"]

        # gather latest deployment details
        deployments = (
            self.client.list_model_deployments(
                project_id=project_id,
                model_id=model_id,
                build_id=build_id,
                async_req=True,
                page_size = 50
            )
            .get()
            .to_dict()
        )
        deployment_info = deployments["model_deployments"][-1]  # most recent deployment

        model_deployment_crn = deployment_info["crn"]

        return {
            "model_id": model_id,
            "model_crn": model_crn,
            "model_access_key": model_access_key,
            "latest_build_id": build_id,
            "latest_deployment_crn": model_deployment_crn,
        }

    def get_latest_standard_runtime(self):
        """
        Use CML APIv2 to identify and return the latest version of a Python 3.7,
        Standard, Workbench Runtime
        """

        try:
            runtime_criteria = {
                "kernel": "Python 3.7",
                "edition": "Standard",
                "editor": "Workbench",
            }
            runtimes = self.client.list_runtimes(
                search_filter=json.dumps(runtime_criteria)
            ).to_dict()["runtimes"]

            versions = {
                version.parse(rt["full_version"]): i for i, rt in enumerate(runtimes)
            }
            latest = versions[max(versions.keys())]

            return runtimes[latest]["image_identifier"]

        except:
            logger.info("No matching runtime available.")
            return None

    def get_all_model_endpoint_details(self):
        """
        Use CML APIv2 to collect all model details required to analyze model metrics
        This function only works for models deployed within the current project.
        """

        Model_AccessKey = self.get_latest_deployment_details_allmodels()["model_access_key"]
        Deployment_CRN = self.get_latest_deployment_details_allmodels()["latest_deployment_crn"]
        Model_CRN = self.get_latest_deployment_details_allmodels()["model_crn"]

        # Get the various Model Endpoint details
        HOST = os.getenv("CDSW_API_URL").split(":")[0] + "://" + os.getenv("CDSW_DOMAIN")
        model_endpoint = (
            HOST.split("//")[0] + "//modelservice." + HOST.split("//")[1] + "/model"
        )

        return Model_AccessKey, Deployment_CRN, Model_CRN, model_endpoint

    def get_model_metrics(self, Model_CRN, Deployment_CRN, db_backup=False):
        """
        Use CML APIv2 to collect all model metrics provided CML Model endpoint details,
        This function only works for models deployed within the current project.
        """

        model_metrics = cdsw.read_metrics(
            model_crn=Model_CRN, model_deployment_crn=Deployment_CRN
        )

        # This is a handy way to unravel the dict into a big pandas dataframe
        metrics_df = pd.io.json.json_normalize(model_metrics["metrics"])

        # Write the data to SQL lite for visualization
        if db_backup == True:
            if not (os.path.exists("model_metrics.db")):
                conn = sqlite3.connect("model_metrics.db")
                metrics_df.to_sql(name="model_metrics", con=conn)

        # Do some conversions & calculations on the raw metrics
        metrics_df["startTimeStampMs"] = pd.to_datetime(
            metrics_df["startTimeStampMs"], unit="ms"
        )
        metrics_df["endTimeStampMs"] = pd.to_datetime(metrics_df["endTimeStampMs"], unit="ms")
        metrics_df["processing_time"] = (
            metrics_df["endTimeStampMs"] - metrics_df["startTimeStampMs"]
        ).dt.microseconds * 1000

        return metrics_df

    def unravel_metrics_df(self, metrics_df):
        """
        Parse metrics_df outputting X, y, formatted for model training
        This function only works for models deployed within the current project.
        """

        y = metrics_df['metrics.final_label'].dropna()
        y = y.astype("int")
        X = metrics_df.filter(like="input_data").dropna().drop(columns=['metrics.input_data.conversion'])
        X.columns = X.columns.str.replace('metrics.input_data.','')

        return X, y

    def load_latest_model_version(self, model_dir="/home/cdsw/01_ML_Project_Basics/models"):
        """
        Load the latest model version in the project.
        This function only works for models deployed within the current project.
        """

        models_list = os.listdir(model_dir)
        models_dates_list = [model_path.replace(".sav","") for model_path in models_list if "final" in model_path]
        model_dates = [int(i.split('_')[2]) for i in models_dates_list]
        latest_model_index = np.argmax(model_dates)
        latest_model_path = model_dir + "/" + models_list[latest_model_index]

        loaded_model = pickle.load(open(latest_model_path, 'rb'))

        return loaded_model

    def store_latest_model_version(self, loaded_model, model_dir="/home/cdsw/01_ML_Project_Basics/models"):
        """
        Store the latest model version in the project.
        This function only works for models deployed within the current project.
        """

        now = time.time()
        filename = model_dir + "/final_model_{}.sav".format(round(now))

        pickle.dump(model, open(filename, 'wb'))

        print("Model backed up with path {}".format(filename))

    def create_model_request(self):
        """
        Create a New CML Model Endpoint.
        This function only works for models deployed within the current project.
        """

        rand_id = "".join([random.choice(string.ascii_lowercase) for _ in range(6)])
        modelReq = cmlapi.CreateModelRequest(
            name = "demo-model-" + rand_id,
            description = "model created for demo",
            project_id = project_id,
            disable_authentication = True
        )

        return modelReq

    def create_model_endpoint(self, modelReq):
        """
        Create a New CML Model Endpoint.
        This function only works for models deployed within the current project.
        """

        model = self.client.create_model(modelReq, self.project_id)

        return model

    def create_model_build_request(self, model, runtime_id):
        """
        Create a New CML Model Build Request.
        Requires a Model Endpoint.
        This function only works for models deployed within the current project.
        """

        model_build_request = cmlapi.CreateModelBuildRequest(
            project_id = project_id,
            model_id = model.id,
            comment = "test comment",
            file_path = self.base_model_file_path,
            function_name = self.function_name,
            kernel = "python3",
            runtime_identifier = runtime_id
        )

        return model_build_request

    def create_model_build(self, model, model_build_request):
        """
        Create a New CML Model Build.
        Requires a Model Build Request object and a Model Endpoint object.
        This function only works for models deployed within the current project.
        """

        modelBuild = client.create_model_build(
            model_build_request, self.project_id, model.id
        )

        return modelBuild

    def create_model_deployment_request(self, model, modelBuild, cpu, mem):
        """
        Create a New CML Model Deployment Request.
        Requires a Model Build object and a Model Endpoint object.
        This function only works for models deployed within the current project.
        """

        model_deployment = cmlapi.CreateModelDeploymentRequest(
            project_id = self.project_id,
            model_id = model.id,
            build_id = modelBuild.id,
            cpu = cpu,
            memory = mem
        )

        return model_deployment

    def create_model_deployment(self, model_deployment, model_id, build_id):
        """
        Create a New CML Model Deployment.
        Requires a Model Deployment Request, a Model Build object and a Model Endpoint object.
        This function only works for models deployed within the current project.
        """

        model_deployment_response = client.create_model_deployment(
                model_deployment,
                project_id = self.project_id,
                model_id = model.id,
                build_id = modelBuild.id
            )

        return model_deployment_response
