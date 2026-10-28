import pyarrow as pa

TIMESTAMP_TYPE = pa.timestamp("us", tz="UTC")


ORG_DIM_SCHEMA = pa.schema([
    pa.field("org_id", pa.int64(), nullable=False),
    pa.field("org_name", pa.string(), nullable=False),
    pa.field("description", pa.string()),
    pa.field("company", pa.string()),
    pa.field("location", pa.string()),
    pa.field("email", pa.string()),
    pa.field("is_verified", pa.bool_(), nullable=False),
    pa.field("followers", pa.int32(), nullable=False),
    pa.field("following", pa.int32(), nullable=False),
    pa.field("public_repos", pa.int32(), nullable=False),
    pa.field("created_at", TIMESTAMP_TYPE, nullable=False),
    pa.field("updated_at", TIMESTAMP_TYPE, nullable=False),
    pa.field("ingestion_timestamp", TIMESTAMP_TYPE, nullable=False),
    pa.field("batch_id", pa.string(), nullable=False),
    pa.field("source", pa.string(), nullable=False),
])

REPO_DIM_SCHEMA = pa.schema([
    pa.field("org_id", pa.int64(), nullable=False),
    pa.field("repo_id", pa.int64(), nullable=False),
    pa.field("name", pa.string(), nullable=False),
    pa.field("full_name", pa.string(), nullable=False),
    pa.field("primary_language", pa.string()),
    pa.field("repo_url", pa.string(), nullable=False),
    pa.field("is_private", pa.bool_(), nullable=False),
    pa.field("description", pa.string()),
    pa.field("isForked", pa.bool_(), nullable=False),
    pa.field("forks_count", pa.int32(), nullable=False),
    pa.field("visibility", pa.string(), nullable=False),
    pa.field("size", pa.int32(), nullable=False),
    pa.field("watchers_count", pa.int32(), nullable=False),
    pa.field("subscribers_count", pa.int32(), nullable=False),
    pa.field("stargazers_count", pa.int32(), nullable=False),
    pa.field("open_issues_count", pa.int32(), nullable=False),
    pa.field("created_at", TIMESTAMP_TYPE, nullable=False),
    pa.field("updated_at", TIMESTAMP_TYPE, nullable=False),
    pa.field("ingestion_timestamp", TIMESTAMP_TYPE, nullable=False),
    pa.field("batch_id", pa.string(), nullable=False),
    pa.field("source", pa.string(), nullable=False),
])

REPO_LANGUAGE_BRIDGE_SCHEMA = pa.schema([
    pa.field("org_id", pa.int64(), nullable=False),
    pa.field("repo_id", pa.int64(), nullable=False),
    pa.field("language", pa.string(), nullable=False),
    pa.field("weightage", pa.float64(), nullable=False),
    pa.field("ingestion_timestamp", TIMESTAMP_TYPE, nullable=False),
    pa.field("batch_id", pa.string(), nullable=False),
    pa.field("source", pa.string(), nullable=False),
])

LANGUAGE_DIM_SCHEMA = pa.schema([
    pa.field("language_name", pa.string(), nullable=False),
    pa.field("language_description", pa.string(), nullable=False),
    pa.field("ingestion_timestamp", TIMESTAMP_TYPE, nullable=False),
    pa.field("batch_id", pa.string(), nullable=False),
    pa.field("source", pa.string(), nullable=False),
])

COMMITS_FACT_SCHEMA = pa.schema([
    pa.field("commit_sha", pa.string(), nullable=False),
    pa.field("author_user_id_verified", pa.int64()),
    pa.field("committer_user_id_verified", pa.int64()),
    pa.field("is_commit_verified", pa.bool_(), nullable=False),
    pa.field("commit_message", pa.string(), nullable=False),
    pa.field("commit_url", pa.string(), nullable=False),
    pa.field("comments_at_ingestion", pa.int32(), nullable=False),
    pa.field("author_name_unverified", pa.string()),
    pa.field("committer_name_unverified", pa.string()),
    pa.field("org_id", pa.int64(), nullable=False),
    pa.field("repo_id", pa.int64(), nullable=False),
    pa.field("committed_at", TIMESTAMP_TYPE, nullable=False),
    pa.field("authored_at", TIMESTAMP_TYPE, nullable=False),
    pa.field("ingestion_timestamp", TIMESTAMP_TYPE, nullable=False),
    pa.field("batch_id", pa.string(), nullable=False),
    pa.field("source", pa.string(), nullable=False),
])

PULL_REQUESTS_FACT_SCHEMA = pa.schema([
    pa.field("org_id", pa.int64(), nullable=False),
    pa.field("repo_id", pa.int64(), nullable=False),
    pa.field("user_raised_id", pa.int64(), nullable=False),
    pa.field("pull_request_id", pa.int64(), nullable=False),
    pa.field("pull_request_url", pa.string(), nullable=False),
    pa.field("source_branch", pa.string(), nullable=False),
    pa.field("target_branch", pa.string(), nullable=False),
    pa.field("is_draft", pa.bool_(), nullable=False),
    pa.field("state", pa.string(), nullable=False),
    pa.field("title", pa.string(), nullable=False),
    pa.field("created_at", TIMESTAMP_TYPE, nullable=False),
    pa.field("updated_at", TIMESTAMP_TYPE, nullable=False),
    pa.field("closed_at", TIMESTAMP_TYPE),
    pa.field("merged_at", TIMESTAMP_TYPE),
    pa.field("pr_comments", pa.int32(), nullable=False),
    pa.field("review_comments", pa.int32(), nullable=False),
    pa.field("commit_count", pa.int32(), nullable=False),
    pa.field("additions", pa.int32(), nullable=False),
    pa.field("deletions", pa.int32(), nullable=False),
    pa.field("changed_files", pa.int32(), nullable=False),
    pa.field("ingestion_timestamp", TIMESTAMP_TYPE, nullable=False),
    pa.field("batch_id", pa.string(), nullable=False),
    pa.field("source", pa.string(), nullable=False),
])

USER_DIM_SCHEMA = pa.schema([
    pa.field("user_id", pa.int64(), nullable=False),
    pa.field("user_name", pa.string(), nullable=False),
    pa.field("user_display_name", pa.string()),
    pa.field("url", pa.string(), nullable=False),
    pa.field("company", pa.string()),
    pa.field("blog", pa.string()),
    pa.field("location", pa.string()),
    pa.field("email", pa.string()),
    pa.field("is_hireable", pa.bool_()),
    pa.field("bio", pa.string()),
    pa.field("created_at", TIMESTAMP_TYPE, nullable=False),
    pa.field("updated_at", TIMESTAMP_TYPE, nullable=False),
    pa.field("ingestion_timestamp", TIMESTAMP_TYPE, nullable=False),
    pa.field("batch_id", pa.string(), nullable=False),
    pa.field("source", pa.string(), nullable=False),
])

ARROW_SCHEMAS = {
    "org": ORG_DIM_SCHEMA,
    "repo": REPO_DIM_SCHEMA,
    "repo_language": REPO_LANGUAGE_BRIDGE_SCHEMA,
    "language": LANGUAGE_DIM_SCHEMA,
    "commits": COMMITS_FACT_SCHEMA,
    "pull_requests": PULL_REQUESTS_FACT_SCHEMA,
    "user": USER_DIM_SCHEMA,
}
