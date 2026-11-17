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