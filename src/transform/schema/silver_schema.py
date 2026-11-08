from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


REPO_LANGUAGE_BRIDGE_SCHEMA = StructType([
    StructField("org_id", LongType(), nullable=False),
    StructField("repo_id", LongType(), nullable=False),
    StructField("language", StringType(), nullable=False),
    StructField("weightage", DoubleType(), nullable=False),
    StructField("source", StringType(), nullable=False),
    StructField("bronze_batch_id", StringType(), nullable=False),
    StructField("bronze_ingestion_timestamp", TimestampType(), nullable=False),
    StructField("silver_batch_id", StringType(), nullable=False),
    StructField("silver_ingestion_timestamp", TimestampType(), nullable=False),
    StructField("valid_from_timestamp", TimestampType(), nullable=False),
    StructField("valid_to_timestamp", TimestampType(), nullable=True),
    StructField("is_active", BooleanType(), nullable=False),
    StructField("is_deleted", BooleanType(), nullable=False),
])

LANGUAGE_DIM_SCHEMA = StructType([
    StructField("language_sk", LongType(), nullable=False),
    StructField("language_name", StringType(), nullable=False),
    StructField("language_description", StringType(), nullable=False),
    StructField("source", StringType(), nullable=False),
    StructField("bronze_batch_id", StringType(), nullable=False),
    StructField("bronze_ingestion_timestamp", TimestampType(), nullable=False),
    StructField("silver_batch_id", StringType(), nullable=False),
    StructField("silver_ingestion_timestamp", TimestampType(), nullable=False),
    StructField("is_deleted", BooleanType(), nullable=False),
])

COMMITS_FACT_SCHEMA = StructType([
    StructField("commit_sha", StringType(), nullable=False),
    StructField("author_sk", IntegerType(), nullable=True),
    StructField("committer_sk", IntegerType(), nullable=True),
    StructField("is_commit_verified", BooleanType(), nullable=False),
    StructField("commit_message", StringType(), nullable=False),
    StructField("commit_url", StringType(), nullable=False),
    StructField("comments_at_ingestion", IntegerType(), nullable=False),
    StructField("author_name_unverified", StringType(), nullable=True),
    StructField("committer_name_unverified", StringType(), nullable=True),
    StructField("org_id", LongType(), nullable=False),
    StructField("repo_id", LongType(), nullable=False),
    StructField("committed_at", TimestampType(), nullable=False),
    StructField("authored_at", TimestampType(), nullable=False),
    StructField("source", StringType(), nullable=False),
    StructField("bronze_batch_id", StringType(), nullable=False),
    StructField("bronze_ingestion_timestamp", TimestampType(), nullable=False),
    StructField("silver_batch_id", StringType(), nullable=False),
    StructField("silver_ingestion_timestamp", TimestampType(), nullable=False),
    StructField("is_deleted", BooleanType(), nullable=False),
])

PULL_REQUESTS_FACT_SCHEMA = StructType([
    StructField("org_id", LongType(), nullable=False),
    StructField("repo_id", LongType(), nullable=False),
    StructField("user_raised_sk", IntegerType(), nullable=False),
    StructField("pull_request_id", LongType(), nullable=False),
    StructField("pull_request_url", StringType(), nullable=False),
    StructField("source_branch", StringType(), nullable=False),
    StructField("target_branch", StringType(), nullable=False),
    StructField("is_draft", BooleanType(), nullable=False),
    StructField("state", StringType(), nullable=False),
    StructField("title", StringType(), nullable=False),
    StructField("created_at", TimestampType(), nullable=False),
    StructField("updated_at", TimestampType(), nullable=False),
    StructField("closed_at", TimestampType(), nullable=True),
    StructField("merged_at", TimestampType(), nullable=True),
    StructField("pr_comments", IntegerType(), nullable=False),
    StructField("review_comments", IntegerType(), nullable=False),
    StructField("commit_count", IntegerType(), nullable=False),
    StructField("additions", IntegerType(), nullable=False),
    StructField("deletions", IntegerType(), nullable=False),
    StructField("changed_files", IntegerType(), nullable=False),
    StructField("source", StringType(), nullable=False),
    StructField("bronze_batch_id", StringType(), nullable=False),
    StructField("bronze_ingestion_timestamp", TimestampType(), nullable=False),
    StructField("silver_batch_id", StringType(), nullable=False),
    StructField("silver_ingestion_timestamp", TimestampType(), nullable=False),
    StructField("is_deleted", BooleanType(), nullable=False),
])

USER_DIM_SCHEMA = StructType([
    StructField("user_sk", LongType(), nullable=False),
    StructField("user_id", LongType(), nullable=False),
    StructField("user_name", StringType(), nullable=False),
    StructField("user_display_name", StringType(), nullable=True),
    StructField("url", StringType(), nullable=False),
    StructField("company", StringType(), nullable=True),
    StructField("blog", StringType(), nullable=True),
    StructField("location", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("is_hireable", BooleanType(), nullable=True),
    StructField("bio", StringType(), nullable=True),
    StructField("created_at", TimestampType(), nullable=False),
    StructField("updated_at", TimestampType(), nullable=False),
    StructField("source", StringType(), nullable=False),
    StructField("bronze_batch_id", StringType(), nullable=False),
    StructField("bronze_ingestion_timestamp", TimestampType(), nullable=False),
    StructField("silver_batch_id", StringType(), nullable=False),
    StructField("silver_ingestion_timestamp", TimestampType(), nullable=False),
    StructField("valid_from_timestamp", TimestampType(), nullable=False),
    StructField("valid_to_timestamp", TimestampType(), nullable=True),
    StructField("is_active", BooleanType(), nullable=False),
    StructField("is_deleted", BooleanType(), nullable=False),
])

SILVER_SCHEMAS = {
    "repo_language": REPO_LANGUAGE_BRIDGE_SCHEMA,
    "language": LANGUAGE_DIM_SCHEMA,
    "commits": COMMITS_FACT_SCHEMA,
    "pull_requests": PULL_REQUESTS_FACT_SCHEMA,
    "user": USER_DIM_SCHEMA,
}

SILVER_PARTITIONS={
    "repo_language": None,
    "language": None,
    "commits": "PARTITIONED BY (days(committed_at))",
    "pull_requests": "PARTITIONED BY (days(created_at))",
    "user": None,
}

SILVER_FOREIGN_KEYS_REPLACEMENTS={
    # if dim_table_type is 'scd2' we also need another key 'time_col' that is used in join cond.
    # time_col should be populated from fact table

    "commits":[
        {
            "dim_table":"user",
            "dim_table_type":"scd2",
            "time_col": "committed_at",
            "join_cols":[{"author_user_id_verified":"user_id"}],  #array of map of fact column to dim column . For more join cond add more elements to array
            'fk':"author_user_id_verified",
            "sk":"user_sk",
            "rename":"author_sk"
        },
        {
            "dim_table":"user",
            "dim_table_type":"scd2",
             "time_col": "committed_at",
            "join_cols":[{"committer_user_id_verified":"user_id"}],  #map of fact column to dim column . For more join cond add more elements to array
            'fk':"committer_user_id_verified",
            "sk":"user_sk",
            "rename":"committer_sk"
        }],


    "pull_requests":[
        {
            "dim_table":"user",
            "dim_table_type":"scd2",
            "time_col": "created_at",
            "join_cols":[{"user_raised_id":"user_id"}],  #array of map of fact column to dim column . For more join cond add more elements to array
            'fk':"user_raised_id",
            "sk":"user_sk",
            "rename":"user_raised_sk"
        },
        ]

}