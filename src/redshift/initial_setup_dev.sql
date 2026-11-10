DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'admin_role') THEN
        CREATE ROLE admin_role;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'viewer_role') THEN
        CREATE ROLE viewer_role;
    END IF;
END
$$;

create SCHEMA IF NOT EXISTS gold;


-- have all access on gold schema in githubanalytics-dev database
GRANT ALL PRIVILEGES ON DATABASE "githubanalytics-dev" TO ROLE admin_role;
GRANT ALL PRIVILEGES ON SCHEMA gold  TO ROLE admin_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO ROLE admin_role;


-- only able to view data on gold schema in githubanalytics-dev database
GRANT USAGE ON DATABASE "githubanalytics-dev" TO ROLE viewer_role;
GRANT USAGE ON SCHEMA gold TO ROLE viewer_role;
GRANT SELECT,REFERENCES ON ALL TABLES IN SCHEMA gold TO ROLE viewer_role;
