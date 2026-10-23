# GitHub extraction datasets ERD

View of the seven extracted datasets. Fact tables are listed first;
the remaining tables provide organization, repository, user, and language
details.

```mermaid
erDiagram
    COMMITS_FACT {
        varchar commit_sha PK
        bigint author_user_id_verified FK
        bigint committer_user_id_verified FK
        boolean is_commit_verified
        text commit_message
        varchar commit_url
        integer comments_at_ingestion
        varchar author_name_unverified
        varchar committer_name_unverified
        bigint org_id FK
        bigint repo_id FK
        timestamp committed_at
        timestamp authored_at
    }

    PULL_REQUESTS_FACT {
        bigint pull_request_id PK
        bigint org_id FK
        bigint repo_id FK
        bigint user_raised_id FK
        varchar pull_request_url
        varchar source_branch
        varchar target_branch
        boolean is_draft
        varchar state
        text title
        timestamp created_at
        timestamp updated_at
        timestamp closed_at
        timestamp merged_at
        integer pr_comments
        integer review_comments
        integer commit_count
        integer additions
        integer deletions
        integer changed_files
    }

    ORG_DIM {
        bigint org_id PK
        varchar org_name
        text description
        varchar company
        varchar location
        varchar email
        boolean is_verified
        integer followers
        integer following
        integer public_repos
        timestamp created_at
        timestamp updated_at
    }

    REPO_DIM {
        bigint org_id PK
        bigint repo_id PK
        varchar name
        varchar full_name
        varchar primary_language
        varchar repo_url
        boolean is_private
        text description
        boolean isForked
        integer forks_count
        varchar visibility
        integer size
        integer watchers_count
        integer subscribers_count
        integer stargazers_count
        integer open_issues_count
        timestamp created_at
        timestamp updated_at
    }

    USER_DIM {
        bigint user_id PK
        varchar user_name
        varchar user_display_name
        varchar url
        varchar company
        varchar blog
        varchar location
        varchar email
        boolean is_hireable
        text bio
        timestamp created_at
        timestamp updated_at
    }

    LANGUAGE_DIM {
        varchar language_name PK
        text language_description
    }

    REPO_LANGUAGE_BRIDGE {
        bigint org_id PK
        bigint repo_id PK
        varchar language PK
        numeric weightage
    }

    ORG_DIM ||--o{ REPO_DIM : owns
    REPO_DIM ||--o{ COMMITS_FACT : contains
    REPO_DIM ||--o{ PULL_REQUESTS_FACT : contains
    USER_DIM o|--o{ COMMITS_FACT : author_or_committer
    USER_DIM ||--o{ PULL_REQUESTS_FACT : raises
    REPO_DIM ||--o{ REPO_LANGUAGE_BRIDGE : languages
    LANGUAGE_DIM ||--o{ REPO_LANGUAGE_BRIDGE : describes
```

Repository tables use the composite key `(org_id, repo_id)`. Commit author and
committer IDs can be null when GitHub does not provide a verified account.
