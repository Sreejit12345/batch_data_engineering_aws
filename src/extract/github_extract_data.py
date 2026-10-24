import requests
import logging
import json
from github_api_client import GitHubClient
from tenacity import retry,wait_exponential,stop_after_attempt
from requests.exceptions import HTTPError
from dotenv import load_dotenv
import os
import boto3
import configparser
import sys

def write_json(d,type):

     with open(f"{type}.json", "w", encoding="utf-8") as f:
        json.dump(d, f,indent=4)


def derive_next_url(resp):
# Parses response, if next present it returns next url to call. Else returns None

    next_call=''
    next_url_=resp.headers

    if next_url_.get('Link') is None:
            return None
    
    next_url=next_url_['Link']

    for url in next_url.split(','):
        if('rel="next' in url.lower()):
            first_angle=url.find('<')
            second_angle=url.find('>')
            next_call=url[first_angle+1:second_angle]
            logger.info(f"Next Url derived is {next_call}")
            return next_call

def retry_if_github_errors(retry_state):
    # if returns True the exponential retry is trigerred else its not
    if retry_state.outcome.failed:
        exception = retry_state.outcome.exception()
        
        if isinstance(exception, HTTPError):
            status_code = exception.response.status_code
            
            if status_code in [429, 500, 502, 503, 504]:
                return True
            
            return False
            
        #Retry other exceptionslike connection drops or timeouts
        return True
        
    #If the call succeeded  do not retry
    return False

@retry(retry=retry_if_github_errors,
          wait=wait_exponential(multiplier=1, min=4, max=10),
       stop=stop_after_attempt(5),reraise=True)

def make_api_call(url,headers):
    # resposible for making api call with exponential backoff
    resp= requests.get(url=url,headers=headers) 
    resp.raise_for_status()
    return resp    
       

def query_org_dimension(client):

     '''
     Responsible to query orgs in org list
     Return a dimension of orgs in org_list

     Natural Key: org_id
     Grain: 1 record per org
     '''

     all_orgs= []

     for o in client.org_list:

          logger.info(f"getting org dimension for org: {o}")

          org_get_url= client.org_base_url + o 

          try:

            resp= make_api_call(url=org_get_url,headers=client.headers).json()
            
            org_dim_dict={
                 "org_id":resp['id'],
                 "org_name":resp['login'],
                 "description":resp["description"],
                 "company":resp['company'],
                 "location":resp["location"],
                 "email":resp["email"],
                 "is_verified":resp["is_verified"],
                 "followers":resp["followers"],
                 "following":resp["following"],
                 "public_repos":resp["public_repos"],
                 "created_at":resp["created_at"],
                 "updated_at":resp["updated_at"]
                 }

            all_orgs.append(org_dim_dict)

          except Exception:
               logger.exception("Failed to get organization details for %s", o)
        
     return all_orgs


def query_repository_dimension(pages_reqd,org_dim,client):

    '''
    Responsible to query repos in org  - also paginates if reqd.
    By default we fetch 100 entries per page, if there are more we continue fetching more pages based on parameter `pages_reqd`
    Returns a list of dict that contains repo dimensional attributes.

    Natural Key: repo_id,org_id
    Grain: 1 record per repo

    '''


    all_repo=[]
 
    for o in org_dim: #for each org list

        i=1

        logger.info(f"getting reops for org {o['org_name']}")

        list_url=client.org_base_url+o['org_name']+'/repos'+"?per_page=100"
        get_url=client.repo_base_url

        while(i<=pages_reqd): #for each page

            logger.info(f"parsing page {i}")

            resp= make_api_call(url=list_url,headers=client.headers)        #get repo list for that org
            _repo_list=resp.json()

            for r in _repo_list: #fetch deep info about each repo listed
                repo_name=r['full_name']
                try:
                    logger.info(f"getting details for repo .... {repo_name}")
                    full_url=get_url+repo_name

                    repo_details=make_api_call(url=full_url,headers=client.headers).json()
                        
                    repo_dim_dict={
                        "org_id": o["org_id"],
                        "repo_id" : repo_details['id'],
                        "name": repo_details['name'],
                        "full_name": repo_details['full_name'],
                        "primary_language":repo_details["language"],
                        "repo_url": repo_details['html_url'],
                        "is_private": repo_details['private'],
                        "description": repo_details['description'],
                        "isForked": repo_details['fork'],
                        "forks_count": repo_details['forks_count'],
                        "visibility": repo_details['visibility'],
                        "size": repo_details['size'],
                        "watchers_count": repo_details['watchers_count'],
                        "subscribers_count": repo_details['subscribers_count'],
                        "stargazers_count": repo_details['stargazers_count'],
                        "open_issues_count": repo_details['open_issues_count'],
                        "created_at": repo_details['created_at'],
                        "updated_at": repo_details['updated_at']
                    }


                    all_repo.append(repo_dim_dict)

                except Exception:
                    logger.exception(
                        "Failed to get details for repository %s",
                        repo_name,
                    )
                             
                
            list_url=derive_next_url(resp)

            if(list_url is None):
                    logger.info(" No next token found .... ")
                    break

            i=i+1

        
    return all_repo


def query_repo_language_bridge(repo_dim,client):
     
     """
     Build a repo language bridge table with weightage percentage.

     Natural Key: repo_id,org_id
     Grain: 1 row per repo/language combination.

     """


     def compute_weight_distribution(language_list):
          # computes weight distribution as ratio for each language used in repo

          total=sum(list(language_list.values()))
          
          for i in language_list.keys():

               language_list[i]=language_list[i]/total

          return language_list


     all_languages=[]
     
     for r in repo_dim:

          org_id= r['org_id']
          repo_id = r['repo_id']

          base_dict= {
                      "org_id":org_id,
                      "repo_id":repo_id,
                    }
                    
          logger.info(f"Getting the language repo bridge  for repo {r['name']}")

          full_url= client.repo_base_url + r['full_name'] + '/languages'

          try:
          
            language_list=make_api_call(full_url,client.headers).json()

            lan_dist=compute_weight_distribution(language_list=language_list)

            for k,v in lan_dist.items():
                
                add_dict={
                        "language":k ,
                        "weightage":v
                }

                all_languages.append( base_dict| add_dict)

          except Exception:
               logger.exception(
                    "Failed to get language bridge data for repository %s",
                    r["full_name"],
               )

     return all_languages


def query_language_dimension(repo_language_bridge,client):
     '''
     # builds a language dimension - right now we dont have rich metadata so we just create a synthetic description. 
     # This is an SCD1 based dimension as we dont need to track any updates on language.

     Grain : One row per language
     Natural Key: language_name
     '''
     
     languages_map = {} 

     for i in repo_language_bridge:
          lang = i['language']
          
          if lang not in languages_map:
               logger.info(f"Getting the language for repo {i['repo_id']}")

               languages_map[lang] = {
                    'language_name': lang,
                    'language_description': f"This is language {lang}"
               }

     return list(languages_map.values())

    
def query_commit_facts_default_branch(pages_reqd,repo_dim,client):
     
     """
     Queries commits on the defualt branch and populates facts incrementally based on watermark.
     Remember, commits are immutable and cant be updated. Only comments can be added later to a commit, but we dont care about it
     - so we call comments metric as comments_at_ingestion.

     Also, we dont get a last updated field in commits, since they are immutable. So even if someone adds comments later its hard
     to ingest the update. So we should do a full pull, but not reqd as this is not an important metric to track.
    
     PK: commit_sha
     FK: org_id,repo_id,author_user_id_verified
     """

     def get_last_watermark():
          return '2025-01-01T00:00:00Z'

     all_commits=[]
     
     for r in repo_dim:

          i=1
          since=get_last_watermark()

          logger.info(f"Getting the commits for repo {r['name']} from {since} ")

          org_id=r["org_id"]
          repo_id=r["repo_id"]

          
          full_url= client.repo_base_url + r['full_name'] + '/commits' + f"?since={since}" +"&per_page=100"

          try:
            while(i<=pages_reqd):

                logger.info(f"parsing page {i}")

                commits_list_resp=make_api_call(full_url,client.headers)
                commits_list_resp_=commits_list_resp.json()

                for commits in commits_list_resp_:

                    commits_fct_dict={
                            "commit_sha": commits["sha"],
                            "author_user_id_verified": commits["author"]["id"] if commits.get("author") else None,
                            "committer_user_id_verified":commits["committer"]["id"] if commits.get("committer") else None,
                            "is_commit_verified":commits["commit"]["verification"]["verified"],
                            "commit_message": commits["commit"]["message"],
                            "commit_url": commits['html_url'],
                            "comments_at_ingestion":commits["commit"]["comment_count"],
                            "author_name_unverified":commits["commit"]["author"]["name"],
                            "committer_name_unverified":commits["commit"]["committer"]["name"], 
                            "org_id":org_id,
                            "repo_id":repo_id,
                            "committed_at":commits['commit']['committer']['date'],
                            "authored_at":commits['commit']['author']['date']
                    }

                    all_commits.append(commits_fct_dict)

                full_url = derive_next_url(commits_list_resp)
                if(full_url is None):
                    logger.info(" No next token found .... ")
                    break

                i=i+1
          except Exception:
              logger.exception(
                  "Failed to extract commits for repository %s",
                  r["full_name"],
              )

     return all_commits


def query_pull_requests_fact(pages_reqd,repo_dim,client):
     
     '''
     Responsible to pull data for all Pull requests. 

     '''

     def is_first_load():
               
               return False
     
     all_pull_requests=[]
        
     for r in repo_dim:
    
            i=1

            since=is_first_load()
            logger.info(f"Getting the pull requests for repo {r['repo_id']}")
    
            org_id=r["org_id"]
            repo_id=r["repo_id"]
    
            if(since==True):
                 state="all"
            else:
                 state="open"

            full_url= client.repo_base_url + r['full_name'] + '/pulls' + f"?state={state}" +"&per_page=100"
     
            while(i<=pages_reqd):

                logger.info(f"parsing page {i}")
                pull_requests_list_resp=make_api_call(full_url,client.headers)
                pull_requests_list_resp_=pull_requests_list_resp.json()

                for pulls in pull_requests_list_resp_:
                    try:
                        get_api_url= client.repo_base_url + r['full_name'] + f'/pulls/{pulls['number']}'
                        
                        logger.info(f" Getting detail info for PR {pulls["title"]}...")
                        
                        get_pr_resp=make_api_call(get_api_url,client.headers).json()


                        pull_requests_fct_dict={
                                "org_id":org_id,
                                "repo_id": repo_id,
                                "user_raised_id": pulls['user']['id'],
                                "pull_request_id": pulls["id"],
                                "pull_request_url": pulls['html_url'],
                                "source_branch":pulls['head']['ref'],
                                "target_branch": pulls['base']['ref'],
                                "is_draft": pulls['draft'],
                                "state": pulls['state'],
                                "title":pulls["title"],
                                'created_at': pulls['created_at'],
                                'updated_at': pulls['updated_at'],
                                "closed_at": pulls['closed_at'],
                                "merged_at": pulls['merged_at'],
                            
                        }

            
                        # new k/v pairs to get from get pull requests
                        get_api_dict={
                                'pr_comments':get_pr_resp['comments'],
                                'review_comments': get_pr_resp['review_comments'],
                                'commit_count':get_pr_resp['commits'],
                                'additions':get_pr_resp['additions'],
                                'deletions':get_pr_resp['deletions'],
                                'changed_files':get_pr_resp['changed_files']
                        }

                        pull_requests_fct_dict.update(get_api_dict)


                        all_pull_requests.append(pull_requests_fct_dict)

                    except Exception:
                        logger.exception(
                            "Failed to process PR %s in repository %s",
                            pulls.get("number"),
                            r["full_name"],
                        )

                full_url = derive_next_url(pull_requests_list_resp)
                if(full_url is None):
                    logger.info(" No next token found .... ")
                    break
    
                i=i+1
    
     return all_pull_requests


def query_user_dimension(commits_fact_incr,query_pull_requests_fact_t,client):

     all_user_id=[]

     all_users_dict=[]

     for c in commits_fact_incr:
          all_user_id.append(c['author_user_id_verified'])
          all_user_id.append(c['committer_user_id_verified'])

     for p in query_pull_requests_fact_t:
          all_user_id.append(p['user_raised_id'])

     deduped_user_id=list(set(all_user_id))

     for u in deduped_user_id:
          full_url= client.user_base_url + f'/{u}'
          logger.info(f"Getting info about user {u}..")


          try:

               user_resp = make_api_call(url=full_url,headers=client.headers).json()

               user_dict={
                    "user_id":u,
                    "user_name": user_resp['login'],
                    "user_display_name": user_resp['name'],
                    "url": user_resp["html_url"],
                    "company": user_resp['company'],
                    "blog": user_resp['blog'],
                    "location": user_resp['location'],
                    "email": user_resp['email'],
                    "is_hireable": user_resp['hireable'],
                    "bio": user_resp['bio'] ,
                    "created_at": user_resp['created_at'],
                    'updated_at': user_resp['updated_at']
               }

               all_users_dict.append(user_dict)

          except Exception:
            logger.exception("Failed to get user details for user ID %s", u)

     return all_users_dict

        

if(__name__=='__main__'):

        # load from env if running locally

        load_dotenv()
        secret=os.getenv('github_pat')
        env= os.getenv('env')

        if ( env is None):

             #read from glue job parameter
             from awsglue.utils import getResolvedOptions
             expected_params = ['env']
             args = getResolvedOptions(sys.argv, expected_params)
             env = args['env']

        config=configparser.ConfigParser()
        config.read(f'src/extract/config/constants-{env}.ini')

        if(secret is None):

             #read from secret manager
             boto_client= boto3.client("secretsmanager", region_name='ap-south-1')
             secret=boto_client.get_secret_value(SecretId=config['general']['secret_name'])['SecretString']

        client_obj = GitHubClient(
             
        headers={'Accept':'application/vnd.github+json',
        'X-GitHub-Api-Version':'2026-03-10',
        'Authorization': f'Bearer {secret}'},

        org_base_url=config['general']['org_base_url'],
        repo_base_url= config['general']['repo_base_url'],
        user_base_url= config['general']['user_base_url'],
        org_list= config['general']['org_list'].split(',')
        )
        
        logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] (%(name)s): %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

        logging.getLogger("urllib3").setLevel(logging.WARNING)

        logger = logging.getLogger("Extract Job")

        org_dim_full =query_org_dimension(client=client_obj)
        repo_dim_full =query_repository_dimension(pages_reqd=10,org_dim=org_dim_full,client=client_obj)
        repo_language_bridge_full = query_repo_language_bridge(repo_dim=repo_dim_full,client=client_obj)
        language_dim_full= query_language_dimension(repo_language_bridge=repo_language_bridge_full,client=client_obj)
        commits_fact_incr=query_commit_facts_default_branch(pages_reqd=10000,repo_dim=repo_dim_full,client=client_obj)
        query_pull_requests_fact_t= query_pull_requests_fact(pages_reqd=10000,repo_dim=repo_dim_full,client=client_obj)
        query_user_dimension_full= query_user_dimension(commits_fact_incr,query_pull_requests_fact_t,client=client_obj)

        logger.info("Job Finished.....")

        print(org_dim_full)


        #write_json(org_dim_full,"src/data/org_dim")
        #write_json(repo_dim_full,"src/data/repo_dim")
        #write_json(commits_fact_incr,"src/data/commits_fact")
        #write_json(repo_language_bridge_full,"src/data/repo_language_bridge")
        #write_json(language_dim_full,'src/data/language_dim')
        #write_json(query_pull_requests_fact_t,'src/data/pull_requests_fact')
        #write_json(query_user_dimension_full,'src/data/user_dim')