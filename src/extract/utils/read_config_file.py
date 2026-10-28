from dotenv import load_dotenv
import os
import sys
import configparser
from pathlib import Path
import src.extract as extract_package
import boto3

def get_env(logger):
    load_dotenv()
    env= os.getenv('env')
    if ( env is None):
            logger.info("No env variable found to derive env - loading from glue...")
            #read from glue job parameter
            from awsglue.utils import getResolvedOptions
            expected_params = ['env']
            args = getResolvedOptions(sys.argv, expected_params)
            env = args['env']
    return env


def get_secret(logger,config):
     load_dotenv()
     secret= os.getenv('github_pat')
     if(secret is None):
        logger.info("No env variable found to derive secret - loading from secret manager...")
        #read from secret manager
        boto_client= boto3.client("secretsmanager", region_name='ap-south-1')
        secret=boto_client.get_secret_value(SecretId=config['general']['secret_name'])['SecretString']
     return secret


def read_config(logger):
    environemnt= get_env(logger)
    config=configparser.ConfigParser()
    config_path = Path(extract_package.__file__).parent / "config" / f"constants-{environemnt}.ini"
    config.read(config_path)

    env_dict={}

    for section in config.sections():
         env_dict[section]=dict(config.items(section))

    env_dict["env"]=environemnt

    return env_dict


def initialize_context_dict(logger):

     logger.info("Initializing context dictionary....")
     config_dict= read_config(logger=logger)
     secret= get_secret(logger=logger,config=config_dict)
     config_dict['secret']=secret

     return config_dict





     
     








