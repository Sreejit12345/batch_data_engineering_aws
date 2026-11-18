# Do note some images have been filtered only to show some year's data

## Total Commits (broken down by verified and unverified) By year and month

```
Query:

select 
year,
month_short_name,
count(*) as total_commit_count,
sum(case when is_commit_verified=TRUE then 1 else 0 end) as verified_commit_count,
sum(case when is_commit_verified=False then 1 else 0 end) as unverified_commit_count
from gold.commit_analysis
where (year >= '${select_year}' OR '${select_year}' IN ('', 'ALL'))
group by year,month_short_name,month_number
order by year,month_number;
```

![Alt text](<screenshots/Commit Volume By Date.png>)

## Total Commits By Repo

```
Query:

select 
org_name,
repo_name,
count(*) as total_commit_count
from gold.commit_analysis
group by org_name,repo_name
order by count(*) desc;
```
![Alt text](<screenshots/Total Commits By Repo.png>)

## Top 10 users with highest commit velocity

```
Query:

with cte as(
select 
user_name,
user_id,
count(*) as total_commit_count
from gold.commit_analysis
where user_name not like 'UNKNOWN'
group by user_name,user_id
)
select * from 
(
select *, row_number() over(order by total_commit_count desc) rn 
from cte
) s
where rn<=10
;
```

![Alt text](<screenshots/Top 10 users with the highest commit count.png>)


## Merged PRs vs Time To Merge

```
Query:

SELECT
    year,
    month_short_name,
    COUNT(*) AS merged_pr_count,
    ROUND(
        AVG(DATEDIFF(second, created_at, merged_at)) / 3600.0,
        2
    ) AS avg_merge_time_hours,

    ROUND(
        AVG(DATEDIFF(second, created_at, merged_at)) / 86400.0,
        2
    ) AS avg_merge_time_days

FROM gold.pull_request_analysis

WHERE merged_at IS NOT NULL
and (year >= '${select_year}' OR '${select_year}' IN ('', 'ALL'))
GROUP BY
    year,
    month_number,
    month_short_name

ORDER BY
    year,
    month_number;
```
![Alt text](<screenshots/Merged PR's vs Time to Merge.png>)



## Abandoned PR by repo (closed but not merged)

```
Query:

SELECT
    repo_id,
    repo_name,
    COUNT(*) AS abandoned_pr_count
FROM gold.pull_request_analysis
WHERE state = 'closed'
  AND merged_at IS NULL
GROUP BY     repo_id,
repo_name
order by  COUNT(*) desc
```
![Alt text](<screenshots/Abandoned PR by repo (closed but not merged).png>)


## Pull requests Funnel (Created=> Open + Merged + Abandoned)

```
Query:

WITH cte AS (
    SELECT *
    FROM gold.pull_request_analysis
),
counts AS (
    SELECT
        COUNT(*) AS created,
        COUNT(CASE WHEN state = 'open' THEN 1 END) AS opened,
        COUNT(CASE WHEN merged_at IS NOT NULL THEN 1 END) AS merged,
        COUNT(CASE
            WHEN state = 'closed' AND merged_at IS NULL
            THEN 1
        END) AS abandoned
    FROM cte
    WHERE created_at IS NOT NULL
),
waterfall AS (
    SELECT 'CREATED' AS stage, created AS display_value,
           created AS waterfall_value, 1 AS sort_order
    FROM counts

    UNION ALL

    SELECT 'OPEN', opened, -opened, 2 FROM counts

    UNION ALL

    SELECT 'MERGED', merged, -merged, 3 FROM counts

    UNION ALL

    SELECT 'ABANDONED', abandoned, -abandoned, 4 FROM counts

    UNION ALL

    SELECT 'REMAINING', 0, 0, 5 FROM counts
)
SELECT
    stage,
    display_value,
    waterfall_value,
    sort_order
FROM waterfall
ORDER BY sort_order;
```
![Alt text](<screenshots/PR Funnel.png>)

## PR Code Churn Analysis

```
Query:

SELECT
    year,
    month_number,
    month_short_name,
    SUM(additions) AS total_lines_added,
    SUM(deletions) AS total_lines_deleted
FROM gold.pull_request_analysis
where (year >= '${select_year}' OR '${select_year}' IN ('', 'ALL'))
GROUP BY
    year,
    month_number,
    month_short_name
ORDER BY
    year,
    month_number;
```
![Alt text](<screenshots/PR code churn analysis.png>)