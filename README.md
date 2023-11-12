# CDE 1.19 ACE Hands-On-Lab Workshop

## About the Hands On Lab Workshops

The Hands-On Lab (HOL) Workshops are an initiative by Cloudera Solutions Engineering aimed at familiarizing CDP users with each Data Service. The content consists of a series of guides and exercises to quickly implement sample end-to-end use cases in the realm of Machine Learning, Datawarehousing, Data Engineering, Data Streaming and Operational Database.

The HOL is typically a three to four-hour event organized by Cloudera for CDP customers and prospects, where a small technical team from Cloudera Solutions Engineering provides cloud infrastructure for all participants and guides them through the completion of the labs with the help of presentations and open discussions.

The HOL contained in this GitHub repository is dedicated to Cloudera Machine Learning. CML is the CDP Data Service for machine learning and AI commonly in Private and Public Clouds.

The content is primarily designed for machine learning engineers, data scientists, and cloud architects. However, little to no code changes are typically required and non-technical stakeholders such as project managers and analysts are encouraged to actively take part.

HOL events are open to all CDP users and customers. If you would like Cloudera to host an event for you and your colleagues please contact your local Cloudera Representative or submit your information [through this portal](https://www.cloudera.com/contact-sales.html). Finally, if you have access to a CDE Virtual Cluster you are welcome to use this guide and go through the same concepts in your own time.

## About the Cloudera Machine Learning (CML) Service

Cloudera Machine Learning (CML) is Cloudera’s platform for machine learning and AI. CML unifies self-service data science and data engineering in a single, portable service as part of an enterprise data cloud for multi-function analytics on data anywhere.

Large scale organizations use CML to build and deploy machine learning and AI capabilities for business at scale, efficiently and securely. CML is built for the agility and power of cloud computing, but can also operate inside your private and secure data center.

## About the Labs

This Hands On Lab is designed to walk you through the Services's main capabilities. Throughout the exercises you will complete a text classification use case and:

1. Use MLFLow Experiments to perform hyperparameter tuning at scale with Spark and PyTorch.
2. Learn about Iceberg's most popular features.
3. Use MLFlow Registry and CML APIv2 to build an MLOps pipeline.
4. Collaborate with other team members to maintain the promoted models and MLOps pipeline in a production-like environment.

## Step by Step Instructions

Detailed instructions are provided in the [step_by_step_guides](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/) folder.

* [Link to the English Guide](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/english).

## Hands On Lab Workshops for Prior Versions of CDE

This GitHub repository pertains to CDE version 1.19 which introduced some important changes with respect to prior versions.

If you don't have access to a CDE 1.19 Virtual Cluster we recommend the prior version of the HOL which covers all versions up until 1.18 and is available [at this GitHub repository](https://github.com/pdefusco/CDE_Tour_ACE_HOL).

## Other CDP Hands On Lab Workshops

CDP Data Services include Cloudera Machine Learning (CML), Cloudera Operational Database (COD), Cloudera Data Flow (CDF) and Cloudera Data Warehouse (CDW). HOL Workshops are available for each of these CDP Data Services.

* [CML Workshop](https://github.com/cloudera/CML_AMP_Churn_Prediction): Prototype and deploy a Churn Prediction classifier, apply an explainability model to it and deploy a Flask Application to share insights with your project stakeholders. This project uses SciKit-Lean, PySpark, and Local Interpretable Model-agnostic Explanations (LIME).
* [CDF Workshop](https://github.com/cloudera-labs/edge2ai-workshop): Build a full OT to IT workflow for an IoT Predictive Maintenance use case with: Edge Flow Management with MQTT and Minifi for data collection; Data Flow management was handled by NiFi and Kafka, and Spark Streaming with Cloudera Data Science Workbench (CDSW) model to process data. The lab also includes content focused on Kudu, Impala and Hue.
* [CDW Workshop](https://github.com/pdefusco/cdw-workshop): As a Big Data Engineer and Analyst for an Aeronautics corporation, build a Data Warehouse & Data Lakehouse to gain an advantage over your competition. Interacticely explore data at scale. Create ongoing reports. Finally move to real-time analysis to anticipate engine failures. All using Apache Impala and Iceberg.
