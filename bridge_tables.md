# Historical vs. Latest-State Joins: Bridge Table Perspective

## 1. What Does the Bridge Table Do?

The `silver.repo_language` table is a bridge table connecting **repositories** with **programming languages**. Because a single repository can contain multiple languages , one repository maps to multiple records in the bridge table, each assigned a proportional weightage.

**Example (`Repository ID: 452221115`):**
* **Python** — Weightage: `0.60`
* **SQL** — Weightage: `0.25`
* **Shell** — Weightage: `0.15`

To track how a repository's language composition evolves over time (SCD Type 2 behavior), the bridge table maintains historical metadata:
* `valid_from_timestamp`
* `valid_to_timestamp`
* `is_active`
* `is_deleted`

---

## 2. Historical Join: Using Bridge Table Validity

In a historical join, the commit timestamp (`committed_at`) is used to resolve which language mappings were active at the exact time the commit occurred.

```sql
JOIN silver.repo_language br
    ON r.repo_id = br.repo_id
   AND c.committed_at >= br.valid_from_timestamp
   AND c.committed_at < br.valid_to_timestamp
```

### How It Works
1. Join commits with the repository dimension using `repo_sk`.
2. Use `repo_id` to locate matching records in the bridge table.
3. Filter by `committed_at` against `valid_from_timestamp` and `valid_to_timestamp` to isolate the snapshot valid at commit time.

> **In simple terms:** Each commit is joined with the corresponding repo version and then attributed to the language distribution that existed when that commit was actually made.

* **Total Weighted Commit Result:** `4544.000000000137`

---

## 3. Latest-State Join: Using Active Bridge Records

In a latest-state join, historical validity windows are bypassed in favor of filtering solely for the currently active bridge records.

```sql
JOIN silver.repo_language br
    ON r.repo_id = br.repo_id
   AND br.is_active = TRUE
```

### How It Works
1. Join commits with the repository using `repo_id`.
2. Filter the bridge table using `br.is_active = TRUE`.
3. Apply the repository's current language distribution retroactively across all commits.

> **In simple terms:** Every historical commit is joined with the latest repo version and it also inherits the repository's current language distribution, regardless of when the commit happened.

* **Total Weighted Commit Result:** `4544.00000000215`

---

## 4. Key Differences

| Feature | Historical Bridge Join | Latest-State Bridge Join |
| :--- | :--- | :--- |
| **Join Condition** | Validity timestamps (`valid_from` / `valid_to`) | Active flag (`is_active = TRUE`) |
| **Mappings Retrieved** | Point-in-time historical language mappings | Current active language mappings only |
| **Distribution Behavior** | Varies across commits as the repo evolves | Uniformly applies current distribution to all commits |
| **Historical Context** | Preserved | Ignored / Overwritten |

---

## 5. Key Observation

The exact total commit count is **4,544**, and both join strategies yield virtually identical total weighted sums (differing only due to floating-point precision on weights summing to `1.0`):
* **Historical Join:** `4544.000000000137`
* **Latest-State Join:** `4544.00000000215`

However, **identical totals do not mean identical distributions**. While the sum across all languages remains `4,544`, the breakdown *per language* will shift significantly if a repository transitioned from one primary language to another over its lifecycle.

If we had repo_sk in the bridge table, the join wouldve have been easier and we could simply join on sk, since we only have natural keys, we join with natural key and time.

---

## 6. Why a Bridge Table Is Needed for Repositories and Languages

In dimensional modeling (star/snowflake schemas), a **bridge table** is essential between `Repository` and `Language` for four primary reasons:

1. **Resolving Many-to-Many (M:N) Relationships:** A single repository uses multiple programming languages, and a single programming language is used across thousands of repositories. Relational fact and dimension tables cannot directly model M:N relationships without duplicating rows or violating normalization.
2. **Preventing Fan-Out / Double Counting:** If you join a `commits` fact table directly to a multi-row language table without a weighted bridge table, a repository with 3 languages would triple-count every commit (`4,544 × 3 = 13,632`). The bridge table stores fractional weights (`0.60 + 0.25 + 0.15 = 1.00`) so aggregated metrics remain accurate.
3. **Storing Relationship-Specific Attributes:** Attributes like `weightage` (percentage of code in that language) belong neither to the repository alone nor to the language alone—they describe the *intersection* of the two.
4. **Decoupling Slowly Changing Dimensions (SCD Type 2):** Languages themselves rarely change, and repository metadata (like repo name or owner) changes independently of its codebase composition. A temporal bridge table isolates changes in language ratios without forcing new surrogate keys (`repo_sk`) in the main repository dimension every time a developer adds a shell script.

---

## 7. Summary Takeaway

The `silver.repo_language` bridge table is more than a static lookup—it captures the evolving relationship between repositories and programming languages:
* Use `valid_from_timestamp` and `valid_to_timestamp` for **accurate historical trend analysis**.
* Use `is_active = TRUE` when you intentionally want to **restate history through the lens of the current tech stack**.