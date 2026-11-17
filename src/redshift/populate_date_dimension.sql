
CREATE OR REPLACE PROCEDURE populate_date_dimension()
LANGUAGE plpgsql
AS $$

BEGIN

CREATE TABLE IF NOT EXISTS gold.dim_date
    (
        date_key       INTEGER,
        full_date      DATE,
        day_of_month   INTEGER,
        day_of_week    INTEGER,
        day_of_week_name VARCHAR(20),
        is_weekend     BOOLEAN,
        week_of_year   INTEGER,
        month_number   INTEGER,
        month_name     VARCHAR(20),
        year           INTEGER,
        quarter        INTEGER
    )
    DISTSTYLE ALL
    SORTKEY (full_date);

  INSERT INTO gold.dim_date
    WITH RECURSIVE base (full_date) AS
    (
        SELECT CAST('1900-01-01' AS DATE) AS full_date

        UNION ALL

        SELECT DATEADD(day, 1, full_date)::DATE
        FROM base
        WHERE full_date < CAST('2099-12-31' AS DATE)
    )

    SELECT
        cast(TO_CHAR(full_date, 'YYYYMMDD') as int) AS date_key,
        full_date,
        EXTRACT(DAY FROM full_date)::INT AS day_of_month,
        EXTRACT(DOW FROM full_date)::INT AS day_of_week,
        TRIM(TO_CHAR(full_date, 'Day')) AS day_of_week_name,

        CASE
            WHEN EXTRACT(DOW FROM full_date) IN (0, 6)
            THEN TRUE
            ELSE FALSE
        END AS is_weekend,

        DATE_PART(week, full_date)::INT AS week_of_year,
        EXTRACT(MONTH FROM full_date)::INT AS month_number,
        TRIM(TO_CHAR(full_date, 'Month')) AS month_name,
        EXTRACT(YEAR FROM full_date)::INT AS year,
        EXTRACT(QUARTER FROM full_date)::INT AS quarter

    FROM base;

END;

$$;

CALL populate_date_dimension();
