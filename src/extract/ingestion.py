from src.extract.github_api_client import *
from src.extract.github_extract_data import *
import logging
import sys
from src.extract.utils.read_config_file import initialize_context_dict
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.catalog import load_catalog
import datetime
from src.extract.schema.schema import ARROW_SCHEMAS
import pyarrow as pa

# database raw-githubanalytics-<env> has already been creasted by cloudformation with location pointing to s3://raw-githubanalytics-<env>/warehouse/
# for local auth the user executing must have aws cli installed and necessary iam permissions .
# In aws glue it is governed by iam role policies

def initialize_catalog(catalog_name,aws_region):

    glue_catalog=load_catalog(
            name=catalog_name,
            **{
                'type': 'glue',
                'client.region': aws_region,
            }
    )
    return glue_catalog


def write_table(table_name,df,logger,database_name,catalog_name,aws_region):

    # appends df to iceberg table 'table_name'. Also creates 1 if it does not exist
    # also for each df passed here we expect a "ingestion_timestamp" paramter which is used in iceberg table partition

    logger.info("Looking up schema for table %s", table_name)

    schema= ARROW_SCHEMAS[table_name]
    catalog = initialize_catalog(catalog_name,aws_region)
    identifier = f"{database_name}.{table_name}"

    if not catalog.table_exists(identifier):

        logger.info(f" Table {identifier} does not exist! creating new table ")
        catalog.create_table(
            identifier=identifier,
            schema=schema,
        )

        table = catalog.load_table(identifier)

        with table.transaction() as txn:
            with txn.update_spec() as spec_upd:
                spec_upd.add_field("ingestion_timestamp", DayTransform(), "ingestion_day")


    table = catalog.load_table(identifier)
    arrow_table = pa.Table.from_pylist(df, schema=table.schema().as_arrow())

    table.append(arrow_table)

    logger.info(f"loaded table {identifier} with {len(arrow_table)} rows")


def clean_up_batch(catalog_name,aws_region,table,database,batch_id,source,ingestion_ts):

    catalog=initialize_catalog(catalog_name,aws_region)

    table = catalog.load_table(f"{database}.{table}")

    table.delete(
        delete_filter=(
            f"batch_id == '{batch_id}' "
            f"AND source == '{source}' "
            f"AND ingestion_timestamp == TIMESTAMP '{ingestion_ts.isoformat()}'"
        )
    )


def load_github_datasets(logger,config_dict):
        
    client_obj = GitHubClient(
    headers={'Accept':'application/vnd.github+json',
    'X-GitHub-Api-Version':'2026-03-10',
    'Authorization': f'Bearer {config_dict["secret"]}'},
    org_base_url=config_dict['general']['org_base_url'],
    repo_base_url= config_dict['general']['repo_base_url'],
    user_base_url= config_dict['general']['user_base_url'],
    org_list= config_dict['general']['org_list'].split(',')
    )

    org_dim_full =query_org_dimension(client=client_obj,logger=logger)
    repo_dim_full =query_repository_dimension(pages_reqd=10,org_dim=org_dim_full,client=client_obj,logger=logger)
    repo_language_bridge_full = query_repo_language_bridge(repo_dim=repo_dim_full,client=client_obj,logger=logger)
    language_dim_full= query_language_dimension(repo_language_bridge=repo_language_bridge_full,client=client_obj,logger=logger)
    commits_fact_incr=query_commit_facts_default_branch(pages_reqd=10000,repo_dim=repo_dim_full,client=client_obj,logger=logger)
    query_pull_requests_fact_t= query_pull_requests_fact(pages_reqd=10000,repo_dim=repo_dim_full,client=client_obj,logger=logger)
    query_user_dimension_full= query_user_dimension(commits_fact_incr,query_pull_requests_fact_t,client=client_obj,logger=logger)


    #write_json(org_dim_full,"src/data/org_dim")
    #write_json(repo_dim_full,"src/data/repo_dim")
    #write_json(commits_fact_incr,"src/data/commits_fact")
    #write_json(repo_language_bridge_full,"src/data/repo_language_bridge")
    #write_json(language_dim_full,'src/data/language_dim')
    #write_json(query_pull_requests_fact_t,'src/data/pull_requests_fact')
    #write_json(query_user_dimension_full,'src/data/user_dim')


    try:
        write_table('org',org_dim_full,logger,config_dict['raw']['database_name'],
                    config_dict['general']['glue_catalog'],config_dict['general']['aws-region'])

        write_table('repo',repo_dim_full,logger,config_dict['raw']['database_name'],
                            config_dict['general']['glue_catalog'],config_dict['general']['aws-region'])

        write_table('repo_language',repo_language_bridge_full,logger,config_dict['raw']['database_name'],
                            config_dict['general']['glue_catalog'],config_dict['general']['aws-region'])

        write_table('language',language_dim_full,logger,config_dict['raw']['database_name'],
                            config_dict['general']['glue_catalog'],config_dict['general']['aws-region'])

        write_table('commits',commits_fact_incr,logger,config_dict['raw']['database_name'],
                            config_dict['general']['glue_catalog'],config_dict['general']['aws-region'])

        write_table('pull_requests',query_pull_requests_fact_t,logger,config_dict['raw']['database_name'],
                            config_dict['general']['glue_catalog'],config_dict['general']['aws-region'])

        write_table('user',query_user_dimension_full,logger,config_dict['raw']['database_name'],
                            config_dict['general']['glue_catalog'],config_dict['general']['aws-region'])

    except Exception as e:

        logger.info("Something went wrong in ingestion")

        logger.info(f"need to delete batch {my_uuid} and ingestion_timestamp {ingestion_ts} for source {source}")

        clean_up_batch(
            config_dict['general']['glue_catalog'],
            config_dict['general']['aws-region'],
            "org",
            config_dict['raw']['database_name'],
            my_uuid,
            source,
            ingestion_ts,
        )
        clean_up_batch(
            config_dict['general']['glue_catalog'],
            config_dict['general']['aws-region'],
            "repo",
            config_dict['raw']['database_name'],
            my_uuid,
            source,
            ingestion_ts,
        )
        clean_up_batch(
            config_dict['general']['glue_catalog'],
            config_dict['general']['aws-region'],
            "repo_language",
            config_dict['raw']['database_name'],
            my_uuid,
            source,
            ingestion_ts,
        )
        clean_up_batch(
            config_dict['general']['glue_catalog'],
            config_dict['general']['aws-region'],
            "language",
            config_dict['raw']['database_name'],
            my_uuid,
            source,
            ingestion_ts,
        )
        clean_up_batch(
            config_dict['general']['glue_catalog'],
            config_dict['general']['aws-region'],
            "commits",
            config_dict['raw']['database_name'],
            my_uuid,
            source,
            ingestion_ts,
        )
        clean_up_batch(
            config_dict['general']['glue_catalog'],
            config_dict['general']['aws-region'],
            "pull_requests",
            config_dict['raw']['database_name'],
            my_uuid,
            source,
            ingestion_ts,
        )
        clean_up_batch(
            config_dict['general']['glue_catalog'],
            config_dict['general']['aws-region'],
            "user",
            config_dict['raw']['database_name'],
            my_uuid,
            source,
            ingestion_ts,
        )

        raise


# Entry point for src to raw ingestion job.
if(__name__=='__main__'):

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    if not root_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] (%(name)s): %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        root_logger.addHandler(handler)

    logging.getLogger("urllib3").setLevel(logging.WARNING)

    logger = logging.getLogger("Extract Job")

    config_dict=initialize_context_dict(logger)

    load_github_datasets(logger,config_dict)

    logger.info("Extract Job Finished.....")