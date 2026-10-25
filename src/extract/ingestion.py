from src.extract.github_api_client import *
from src.extract.github_extract_data import *
import src.extract as extract_package
import os
import logging
from pathlib import Path
from src.extract.write_iceberg import *
import boto3
import sys
import configparser


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


# load from env if running locally

load_dotenv()
secret=os.getenv('github_pat')
env= os.getenv('env')

if ( env is None):
        logger.info("No env variable found - loading from glue...")
        #read from glue job parameter
        from awsglue.utils import getResolvedOptions
        expected_params = ['env']
        args = getResolvedOptions(sys.argv, expected_params)
        env = args['env']

config=configparser.ConfigParser()
config_path = Path(extract_package.__file__).parent / "config" / f"constants-{env}.ini"
config.read(config_path)


if(secret is None):
        logger.info("No env variable found - loading from secret manager...")
        #read from secret manager
        boto_client= boto3.client("secretsmanager", region_name='ap-south-1')
        secret=boto_client.get_secret_value(SecretId=config['general']['secret_name'])['SecretString']


def load_github_datasets():
    client_obj = GitHubClient(
            
    headers={'Accept':'application/vnd.github+json',
    'X-GitHub-Api-Version':'2026-03-10',
    'Authorization': f'Bearer {secret}'},

    org_base_url=config['general']['org_base_url'],
    repo_base_url= config['general']['repo_base_url'],
    user_base_url= config['general']['user_base_url'],
    org_list= config['general']['org_list'].split(',')
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

    write_table('org',org_dim_full,logger,config['bronze']['database_name'],
                config['general']['glue_catalog'],config['general']['aws-region'])

    write_table('repo',repo_dim_full,logger,config['bronze']['database_name'],
                        config['general']['glue_catalog'],config['general']['aws-region'])
   
    write_table('repo_language',repo_language_bridge_full,logger,config['bronze']['database_name'],
                        config['general']['glue_catalog'],config['general']['aws-region'])

    write_table('language',language_dim_full,logger,config['bronze']['database_name'],
                        config['general']['glue_catalog'],config['general']['aws-region'])

    write_table('commits',commits_fact_incr,logger,config['bronze']['database_name'],
                        config['general']['glue_catalog'],config['general']['aws-region'])

    write_table('pull_requests',query_pull_requests_fact_t,logger,config['bronze']['database_name'],
                        config['general']['glue_catalog'],config['general']['aws-region'])

    write_table('user',query_user_dimension_full,logger,config['bronze']['database_name'],
                        config['general']['glue_catalog'],config['general']['aws-region'])


load_github_datasets()



logger.info("Extract Job Finished.....")