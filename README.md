# Raven Features
This repository contains the ClearML-integrated pipeline logic for triggering and managing featurization jobs on data 
pulled from **Raven**. Pipelines are configured dynamically using a **YAML configuration file**, and launched 
per-series based on the results of your Raven queries.

---

## 🚀 Overview
Featurization pipeline runs:

* 🧩 Are fully configurable via **YAML file** stored in S3
* 🖥️ Are triggered from the command line, with only the YAML config S3 URI as an input parameter
* 🔍 Pull valid `series_uid` lists from **Raven** based on the YAML config
* ⚙️ Dynamically construct and configure ClearML pipeline steps based on the YAML config
* 🔁 Launch featurization pipelines **per Series UID**
* ☁️ Run in the cloud environment of your choice based on the YAML config

---

## 📄 Configuration

As referenced above, featurization pipelines are configured using a **YAML file** that conforms to the `PipelineConfig` 
Pydantic model.

* This YAML file must be uploaded to an **S3 location**
* The URI to this file is passed into the CLI command

### Example S3 URI

```
s3://px-app-bucket/config/my-pipeline.yaml
```

### Example YAML Structure

```yaml
project_parameters:
  email: example@picturehealth.com
  project_id: MY-TEST-PROJECT
  feature_group_id: MY_FEATURE_GROUP_ID

autoscaler_parameters:
  launcher_queue: PxPipeline_Launcher

raven_query_parameters:
  modality: radiology
  dataset_id: MY-DATASET-OF-INTEREST
  organ_identifier: LUNG
  organ_mask_provenance: [RAVEN-ETL, RAVEN-NIFTI]
  lesion_mask_provenance: [RAVEN-ETL, RAVEN-NIFTI, RAVEN-ANGO]

pipeline_steps:
  - name: Step 1 Descriptive Name
    repo: https://github.com/Picture-Health/step-1.git
    branch: my-branch-1
    working_directory: .
    commit: f6d880c
    script: pipeline_entrypoint.py
    execution_queue: SampleExecutionQueue1
    parent_steps: []
    time_limit: 1800

  - name: Step 2 Descriptive Name
    repo: https://github.com/Picture-Health/step-2.git
    branch: my-branch-2
    working_directory: .
    commit: b9f61c8
    script: pipeline_entrypoint.py
    execution_queue: SampleExecutionQueue2
    parent_steps: ['Step 1 Descriptive Name']
    time_limit: 1800

  - name: Step 3 Descriptive Name
    repo: https://github.com/Picture-Health/step-3.git
    branch: my-branch-3
    working_directory: .
    commit: s0fsc8d
    script: pipeline_entrypoint.py
    execution_queue: SampleExecutionQueue2
    parent_steps: ['Step 2 Descriptive Name']
    time_limit: 1800

  - name: Step 4 Descriptive Name
    repo: https://github.com/Picture-Health/step-4.git
    branch: my-branch-4
    working_directory: .
    commit: a94nfh6
    script: pipeline_entrypoint.py
    execution_queue: SampleExecutionQueue2
    parent_steps: ['Step 2 Descriptive Name']
    time_limit: 1800
```

---

## 🔧 Usage

Run the pipeline launcher by calling the CLI entrypoint and passing in your config YAML location:

```bash
featurize --config-uri s3://px-app-bucket/config/my-pipeline.yaml
```

This will:

1. Load and validate your YAML config from S3
2. Query Raven for Series UIDs with valid images and masks available based on your criteria
3. Launch a ClearML pipeline task for each matching Series UID

---

## 📁 Directory Structure

```
raven-features/
├── raven_features/
    ├── cli.py                  # CLI entrypoint
    ├── driver.py               # ClearML pipeline driver
├── utils/
    ├── aws.py                  # S3 helper functions
    ├── config.py               # Pydantic model + helpers for YAML config
    ├── decorators.py           # Decorators for validation
    ├── env.py                  # Environment variables
    ├── logs.py                 # Logging setup
    ├── params.py               # Methods for packing and unpacking ClearML parameters
    └── time.py                 # Time formatting functions
```

---

## 🧠 Under the Hood

### `cli.py`

* Starts a ClearML task
* Downloads and validates the YAML config
* Queries Raven for Series UID list
* Launches one ClearML pipeline per UID

### `driver.py`

* Gets triggered by ClearML after `cli.py`
* Rehydrates the config from flattened task params
* Registers individual pipeline steps from config
* Starts ClearML pipeline execution

---

## ✅ Requirements

* Python 3.10+
* ClearML SDK
* Valid S3 config YAML file

---

## 📬 Example Output

Upon running the CLI, logs will include:

```
📥 Loading pipeline config from: s3://px-app-bucket/config/my-pipeline.yaml
🔎 Performing Raven query...
🚀 Launching featurization pipelines...
✅ Featurization pipelines successfully triggered.
```

You can monitor execution in the ClearML dashboard under your project name.
