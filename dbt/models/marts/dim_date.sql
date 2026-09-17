WITH
spine AS (
    SELECT generated_day::date AS date_day
    FROM generate_series(
        '2026-08-26'::date,
        '2030-01-01'::date,
        interval '1 day'
    ) AS t(generated_day)
),

calendar AS (
    SELECT
        date_day,
        EXTRACT(isodow FROM date_day)::int AS day_of_week,
        TO_CHAR(date_day, 'FMDay') AS day_name,
        EXTRACT(isodow FROM date_day) IN (6, 7) AS is_weekend,
        EXTRACT(week FROM date_day)::int AS week_of_year,
        EXTRACT(month FROM date_day)::int AS month_number,
        TO_CHAR(date_day, 'FMMonth') AS month_name,
        EXTRACT(quarter FROM date_day)::int AS quarter,
        EXTRACT(year FROM date_day)::int AS year
    FROM spine
)

SELECT *
FROM calendar
