from src.extract.github_api_client import *
from src.extract.github_extract_data import *
import logging
from src.extract.write_iceberg import *
import boto3
import sys
from src.extract.utils.read_config_file import read_config


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


config_dict=read_config(logger)

print(config_dict)


def load_github_datasets():
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


load_github_datasets()



logger.info("Extract Job Finished.....")