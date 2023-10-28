# Setup Job
#1. get storage
#2. set storage as proj env via cml bootstrap

# create dev proj from git

# create qa proj from git

# create prd proj from git

# create model monitoring proj from git

# create data create job in dev model; store data to cloud storage. with schedule every 5 mins

# create dev job in dev proj. with no schedule dependent on data create job

# create model testing job in qa proj with schedule every 10 minutes

# create model pushing job in qa proj. dependent on create model testing job.

# create proj backup job in prd proj. run every 15 minutes. Backup artifacts from MLFlow proj dependencies.

# create model synthetic requests jobs in model monitoring proj. Schedule the job to run every 3 minutes.

# create model monitoring job in model monitoring prj. Monitor model performance and model skew (evidently AI). Schedule every 10 minutes.

# Manual Steps:
#1. Open CML Session and train model just like in job. Validate training in MLFlow UI Experiments.
#2. Observe factory build models.  
#3. manually trigger model deployment from QA proj.




