CREATE ROLE  admin_role; 
CREATE ROLE  viewer_role; 


-- create external schema that points to glue silver database
create external schema silver 
from data catalog 
database 'silver-githubanalytics-dev'   
IAM_ROLE default;


-- create internal schema that refers external tables . THis is the gold schema, that has views created
create  schema gold ;


-- have all access on gold and silver schema in githubanalytics-dev database
GRANT ALL PRIVILEGES ON DATABASE "githubanalytics-dev" TO ROLE admin_role;

GRANT ALL PRIVILEGES ON SCHEMA gold  TO ROLE admin_role;
GRANT ALL PRIVILEGES ON SCHEMA silver  TO ROLE admin_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO ROLE admin_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO ROLE admin_role;
GRANT CREATE USER TO ROLE admin_role;

-- for future
ALTER DEFAULT PRIVILEGES FOR USER "IAMR:cicd-iam" IN SCHEMA gold GRANT ALL PRIVILEGES ON TABLES TO ROLE admin_role;
ALTER DEFAULT PRIVILEGES FOR USER "IAMR:cicd-iam"  IN SCHEMA silver GRANT ALL PRIVILEGES ON TABLES TO ROLE admin_role;


-- only able to view data on gold and silver schema in githubanalytics-dev database
GRANT ALL PRIVILEGES ON DATABASE "githubanalytics-dev" TO ROLE viewer_role;

GRANT USAGE ON SCHEMA gold TO ROLE viewer_role;
GRANT SELECT,REFERENCES ON ALL TABLES IN SCHEMA gold TO ROLE viewer_role;
GRANT USAGE ON SCHEMA silver TO ROLE viewer_role;
GRANT SELECT,REFERENCES ON ALL TABLES IN SCHEMA silver TO ROLE viewer_role;

-- for future
ALTER DEFAULT PRIVILEGES FOR USER "IAMR:cicd-iam"  IN SCHEMA gold GRANT SELECT, REFERENCES ON TABLES TO ROLE viewer_role;
ALTER DEFAULT PRIVILEGES FOR USER "IAMR:cicd-iam"  IN SCHEMA silver GRANT SELECT, REFERENCES ON TABLES TO ROLE viewer_role;


