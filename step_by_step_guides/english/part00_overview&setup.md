# Part 0: High Level Overview & Workshop Setup

### High Level Overview

This workshop is divided in two parts:

* In part 1 you will familiarize yourself with CML foundational concepts including workspaces, projects, runtimes, and the fundamental components of an MLOps tooling in CML including git integration, Apache Iceberg and MLFlow.
* In part 2 you will create an MLOps pipeline to develop, deploy, monitor and maintain a classifier in order to predict credit card fraud. During this process you will leverage the tools you were introduced to in part 1, but in unison.

### Workshop Setup

In order to deploy this project you can clone this git repository as a CML Project: https://github.com/pdefusco/CML_MLOps_ACE_Workshop

When you are done, launch a CML Session with the following:

```
Editor: Workbench
Kernel: Python 3.9
Resource Profile: 2 CPU / 4 GB Memory / 0 GPU
Spark Addon: Spark 3.2
```

When the session has launched, open the terminal and run the following command:

```
pip3 install -r requirements.txt
```

Congratulations, your project is ready! Please move on to [part 1 - MLFlow Tracking](), the next lab in the step by step guides.
