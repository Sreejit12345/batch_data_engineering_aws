# GitHub Analytics Batch Data Engineering on AWS

This repository contains the application code for the `batch_data_engineering_aws` project. The infrastructure is provisioned via the `batch_data_engineering_aws_infra` repo.

This repo has two GitHub Actions pipelines and the Python source code for the ETL workloads.

## Project overview

The project pulls GitHub metadata for organization-owned repositories, lands it in a medallion architecture on S3, registers the data in the AWS Glue Catalog, and exposes curated Gold-layer views in Redshift Serverless for reporting and dashboarding.

This repo is intentionally designed for controlled, repeatable batch processing rather than streaming. It is not a real-time event-driven pipeline; it is built around batch orchestration, metadata-driven transformations, and cost-conscious use of AWS services.

## Repo explanation, high level

### 1) `src/config`

The `config` folder has `.ini` files based on the environment (`constants-dev.ini`, `constants-prod.ini`, etc.). These provide the environment-specific parameters needed by the Glue jobs, such as `database_name`, secret name, and other operational settings.

This file is parsed differently depending on how the job is triggered:

- For local runs, it expects a `.env` file containing the environment name and GitHub PAT.
- The environment key in the `.env` file is used to read the relevant config.
- In AWS, Glue jobs deployed through CloudFormation have a parameter `--env` that tells the job which environment it is running in.
- The config file is parsed based on the `env` parameter, and the GitHub PAT is derived from AWS Secrets Manager.
- The secret name in Secrets Manager is derived by parsing the ini file.

### 2) `src/data`

The `data` folder contains sample data from local runs to help understand the structure of the responses returned by the GitHub REST APIs.

### 3) `src/extract`

The `extract` folder contains the code for the raw + bronze medallion layers. It also contains a sub-folder called `schema`, which stores the schemas for the raw and bronze layers.

- `ingestion.py` is the entry point for the RAW Glue job
- `changelog.py` is the entry point for the BRONZE Glue job
- Other files are helper modules

### 4) `src/transform`

The `transform` folder contains the Silver layer code, including the SCD1/SCD2 logic. `silver_transform.py` is the entry point for the SILVER Glue job. It also contains a `schema` folder with the schema for the Silver layer.

### 5) `src/utils`

The `utils` folder contains the shared generic modules used across the codebase.

- `read_config_file.py` contains logic for parsing the environment based on the env file or Glue job parameter, and deriving the GitHub PAT from the environment.
- `common_functions.py` contains reusable generic Spark code.

### 6) `src/redshift`

The `redshift` folder contains the SQL code for the Gold layer. It has init scripts such as `initial_setup_dev.sql`, which help set up initial roles and access control for Gold objects. It also contains gold-layer views on which dashboards are built in Amazon QuickSight.

### 7) `setup.py`

`setup.py` helps build the wheel file, which is pushed to the source code S3 bucket and made available to the Glue jobs as a Python dependency.

## GitHub Actions CI/CD pipelines

1) `source_code_deployment`  
   This pipeline is responsible for building the Python wheel file and uploading the entry-point files (the `.py` files) to the environment-specific source code S3 bucket. The Glue jobs refer to these files during execution.

2) `redshift_serverless_role_setup_one_time`  
   This pipeline is responsible for setting up the Redshift Serverless roles for access. It is intended to be executed `once per environment`.

   When we create the Redshift Serverless namespace and workspace through CloudFormation, it automatically creates an admin username and password and stores it in AWS Secrets Manager. When the pipeline runs, it expects a default parameter named `redshift_serverless_arn`, which is the ARN of the admin secret created by Redshift during setup. We need to copy this ARN and provide it during the run.

   Branch names have utmost importance here. If we push to the `dev` branch, it deploys the specific source code bucket and sets up roles for that specific Redshift Serverless database. Currently  its enabled for deployment on dev and prod branch.

## Architecture

See [architecture.md](./architecture.md) for the full deployment and pipeline flow.

## Medallion layer principles followed

See [medallion.pdf](./medallion.pdf).

## What this project does

- Pulls GitHub API data into a raw landing layer
- Converts raw payloads into Bronze tables with schema validation and audit fields
- Applies metadata-driven SCD1 / SCD2 logic in the Silver layer
- Publishes curated Gold views for downstream reporting and QuickSight usage
- Uses a batch-oriented Glue + S3 architecture rather than a streaming platform

This is intentionally designed for controlled, repeatable batch loads rather than near-real-time ingestion.

## Architecture at a glance

- GitHub API -> AWS Glue ingestion jobs
- Raw data -> Bronze data -> Silver transformation
- Glue Data Catalog registration
- Redshift Serverless for Gold reporting layer
- Step Functions + SNS for orchestration and notifications

## Repository structure

### Source code

- `src/config/`  
  Environment-specific configuration files (`*.ini`) used by Glue jobs. These are selected by environment, with local runs using a `.env` file and AWS jobs using an `--env` parameter.

- `src/data/`  
  Sample payloads and local test data used to understand the structure of GitHub API responses.

- `src/extract/`  
  Raw and Bronze layer logic. Includes the entry points for ingestion and changelog processing, plus schema definitions for Raw/Bronze datasets.

- `src/transform/`  
  Silver layer logic for dimensional and fact updates, including SCD1 and SCD2 handling.

- `src/utils/`  
  Shared utility modules for config parsing, secrets handling, and common Spark functions.

- `src/redshift/`  
  Gold-layer SQL assets, including setup and access scripts for Redshift Serverless views.

### Packaging and deployment

- `setup.py`  
  Builds the Python wheel used by AWS Glue jobs.

- `.github/workflows/`  
  CI/CD pipelines for deployment automation.

## Local setup

1. Clone the repository.
2. Create a virtual environment.
3. Install Python dependencies.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If you need to build the distribution package locally:

```bash
python setup.py bdist_wheel
```

## Configuration model

The project uses environment-specific config files under `src/config`, such as:

- `constants-dev.ini`
- `constants-prod.ini`
- other env-specific variants as needed

These files hold the runtime parameters used by the jobs, such as:

- database names
- secret names
- organization lists
- environment-specific settings

Make sure config file is present for the env being deployed to.

### Local vs AWS execution

For local execution, the code expects a `.env` file containing the active environment and the GitHub PAT. In AWS, the Glue jobs receive an `--env` parameter, and the code resolves the necessary configuration and secret name from AWS Secrets Manager based on that environment.

## Data engineering conventions

The project follows several practical conventions that are important for correctness and maintainability:

1) We track only repos belonging to organizations. This is controlled by the `org_list` in the config file. It expects a comma-separated value of repos to track.
2) All timestamps and dates are in UTC format.
3) GitHub APIs have hard deletes, so we do delta-by-pk full snapshot.
4) Metadata-driven framework: raw table names are the same as bronze and silver table names. The database names are different for each layer.
5) Metadata-driven framework: in dimensions, the SCD1/SCD2 surrogate key column created by the framework is always `<tablename>_sk`.
6) Column names are the same in all layers except audit columns.
7) In Silver SCD2 tables, there must be `valid_from_timestamp`, `valid_to_timestamp`, `is_active`, and `is_deleted` columns.
8) In Silver SCD1 tables, there must be an `is_deleted` column.
9) Natural keys for each dataset are expected to be non-null.
10) All dims are generated with `-99` surrogate keys to help join unknown facts.
11) Concurrency is `1` for all jobs. We do batch-by-batch processing. This is not meant to be a streaming or parallel processing framework.
12) Supported table types: `dim` and `fact`; supported load types: `scd1` and `scd2` for the metadata framework. SCD2 loads expect a column name `is_updated` to work.
13) Complex cases like bridge tables and multi-join tables are processed independently.
14) In bronze-silver metadata, we ensure dims are loaded before facts so that key replacement is done properly.
15) Fact tables in Silver have their FKs replaced by surrogate keys. These are mentioned in the key replacement constants.
16) Right now all 7 datasets are processed in a loop in one raw/bronze and silver job to save costs.
17) Metadata for batch progress is maintained in the metadata database and metadata S3 bucket.

## Notes

- This project is structured for controlled AWS deployment and environment-specific config management.
- The architecture is designed around a repeatable medallion data pipeline, not ad hoc transformations.
- It is best suited for regular scheduled data loads and curated warehouse reporting rather than event-driven streaming.

## Related docs

- [architecture.md](./architecture.md)
- [medallion.pdf](./medallion.pdf)
