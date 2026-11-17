# GitHub Analytics Batch Data Engineering on AWS

This repository contains the application code for the `batch_data_engineering_aws` project. The infrastructure is provisioned via the `batch_data_engineering_aws_infra` repo.

This repo has two GitHub Actions pipelines and the Python source code for the ETL workloads.

## Project overview

The project pulls GitHub metadata from the GitHub REST APIs, lands it in a medallion architecture on S3, registers the data in the AWS Glue Catalog, and exposes curated Gold-layer views in Redshift Serverless for reporting and dashboarding.

This repo is designed for controlled, repeatable batch processing rather than streaming. It is not a real-time event-driven pipeline; it is built around batch orchestration, metadata-driven transformations, and cost-conscious use of AWS services.

## Repository structure and flow

This repo is built around the medallion architecture and the environment-specific AWS deployment setup. The code is split so the config, ingestion, transformation, and reporting pieces stay clear, reusable, and easy to work with across environments.

### 1) `src/config`

The `config` folder has the environment-specific `.ini` files like `constants-dev.ini` and `constants-prod.ini`. These hold the runtime values the Glue jobs need, like database name, secret name, and other env-specific settings.

The config logic is built to work in both local and AWS runs:

- For local runs, it expects a `.env` file with the `env` and `github_pat` keys.
- In AWS, Glue jobs use the `--env` parameter to know which environment they are running in. This is set by CloudFormation.
- The config file is parsed based on `env`, and the GitHub PAT is pulled from AWS Secrets Manager.
- The secret name in Secrets Manager is derived from the environment-specific ini file itself.


### 2) `src/data`

The `data` folder has sample payloads from local runs. These help understand the structure of the GitHub REST API responses and also help validate the downstream transformations.

### 3) `src/extract`

The `extract` layer holds the logic for the Raw and Bronze medallion stages. It also has a `schema` subfolder with the schema definitions for the raw and bronze datasets.

Main pieces:

- `ingestion.py` is the entry point for the RAW Glue job
- `changelog.py` is the entry point for the BRONZE Glue job
- Other modules handle the reusable extraction, validation, and load logic

This layer is responsible for pulling GitHub entities, landing the raw data, and turning it into the Bronze structure with schema checks and audit fields.

### 4) `src/transform`

The `transform` folder holds the Silver layer logic, including the metadata-driven SCD1 and SCD2 processing. `silver_transform.py` is the entry point for the SILVER Glue job.

It also has a `schema` folder with the Silver-layer schema definitions. This logic is responsible for:

- reconciling historical changes
- applying SCD1 / SCD2 behavior
- replacing surrogate keys for fact tables
- keeping the data model aligned with the downstream Gold-layer reporting

### 5) `src/utils`

The `utils` folder holds the common functionality used across the codebase.

- `read_config_file.py` handles the environment detection from `.env` or Glue job parameters and resolves the GitHub PAT based on the active environment.
- `common_functions.py` contains shared Spark helpers and common transformation utilities used across jobs.

This keeps the ETL logic cleaner and avoids repeating the same logic across ingestion and transformation stages.

### 6) `src/redshift`

The `redshift` folder has the SQL assets for the Gold layer. It includes setup scripts like `initial_setup_dev.sql`, which help set up the initial roles and access control for Gold objects. `initial_setup_dev.sql` connects to redshift cluster using redshift data api, using the admin credentials stored in aws secret manager.


It also contains Gold-layer views used for dashboarding and business reporting.These are created by the cicd account specific role, and since admin_role and viewer_roles in DB have default privileges, users part of those roles will be able to access those objects.

### 7) `setup.py`

`setup.py` builds the Python wheel for the project. That package is pushed to the source-code S3 bucket and made available to the Glue jobs as a Python dependency.

This packaging step is important for deploying the ETL jobs consistently across environments.

## GitHub Actions CI/CD pipelines

1) `source_code_deployment`  
   This pipeline is responsible for building the Python wheel file and uploading the entry-point files (the `.py` files) to the environment-specific source code S3 bucket. The Glue jobs refer to these files during execution. Currently  its enabled for deployment on `dev` and `prod` branch.

2) `redshift_serverless_role_setup_one_time`  
   This pipeline is responsible for setting up the Redshift Serverless roles for access. It is intended to be executed `once per environment`.

   When we create the Redshift Serverless namespace and workspace through CloudFormation, it automatically creates an admin username and password and stores it in AWS Secrets Manager. When the pipeline runs, it expects a default parameter named `redshift_serverless_arn`, which is the ARN of the admin secret created by Redshift during setup. We need to copy this ARN and provide it during the run.

3) `source_code_deploy_redshift` 
   This pipeline is responsible for deploying the gold objects to redshift serverless. This connects to redshift serverless using redshift data api, using the account level IAM cicd role that we set up in the setup stack. Currently  its enabled for deployment on `dev` and `prod` branch.

  Branch names have utmost importance here. If we push to the `dev` branch, it deploys to the specific source code bucket and sets up roles for that specific Redshift Serverless database for that env. Similarly it deploys gold objects to that specific redshift env.


## Architecture

See [architecture.md](./architecture.md) for the full deployment and pipeline flow.

## Medallion layer principles followed

See [medallion.pdf](./medallion.pdf).


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
