class GitHubClient:

    def __init__(self,headers,org_base_url,repo_base_url,user_base_url,org_list):
        self.headers =headers
        self.org_base_url= org_base_url
        self.repo_base_url =  repo_base_url
        self.user_base_url = user_base_url
        self.org_list=org_list
    