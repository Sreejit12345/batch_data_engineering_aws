CREATE OR REPLACE VIEW gold.commit_analysis
AS
with cte as (
select 
c.commit_sha,
c.commit_url,
c.is_commit_verified,
d.full_date,
d.year,
d.month_short_name,
d.month_number,
r.repo_id, -- natural key
r.name as repo_name,
r.primary_language as repo_primary_language,
r.description as repo_description,
o.org_id, -- natural key
o.org_name as org_name,
o.description as org_desc,
u.user_id,  -- natural key
u.user_name ,
u.user_display_name
from silver.commits c
join gold.dim_date d
on 
(CAST(TO_CHAR(c.committed_at, 'YYYYMMDD') AS INTEGER) = d.date_key) -- date dim
join silver.repo r
on(c.repo_sk=r.repo_sk) -- repo dim 
join silver.org o
on(c.org_sk=o.org_sk) -- org dim 
join silver.user u
on(c.author_sk=u.user_sk)  -- user dim
where c.is_deleted=FALSE
)
select * from cte
WITH NO SCHEMA BINDING;



CREATE OR REPLACE VIEW gold.pull_request_analysis
AS 
WITH cte AS (
    SELECT
        p.pull_request_id,
        p.pull_request_url,
        p.source_branch,
        p.target_branch,
        p.is_draft,
        p.state,
        p.title,
        p.pr_comments,
        p.review_comments,
        p.commit_count,
        p.additions,
        p.deletions,
        p.changed_files,
        d.full_date,
        d.year,
        d.month_short_name,
        d.month_number,
        r.repo_id, -- natural key
        r.name AS repo_name,
        r.primary_language AS repo_primary_language,
        r.description AS repo_description,
        o.org_id, -- natural key
        o.org_name,
        o.description AS org_desc,
        u.user_id, -- natural key
        u.user_name,
        u.user_display_name,
        p.created_at,
        p.updated_at,
        p.closed_at,
        p.merged_at

    FROM silver.pull_requests p

    JOIN gold.dim_date d
        ON (
            CAST(TO_CHAR(p.created_at, 'YYYYMMDD') AS INTEGER) = d.date_key
        )

    JOIN silver.repo r
        ON (p.repo_sk = r.repo_sk)

    JOIN silver.org o
        ON (p.org_sk = o.org_sk)

    JOIN silver.user u
        ON (p.user_raised_sk = u.user_sk)

    WHERE p.is_deleted = FALSE
)

SELECT *
FROM cte
WITH NO SCHEMA BINDING;;