WITH
source AS (
    SELECT *
    FROM {{ source('steam', 'app_metadata') }}
),

renamed AS (
    SELECT
        app_id,
        (data->>'steam_appid')::bigint AS steam_appid,
        data->>'name' AS app_name,
        data->>'type' AS app_type,
        data->>'short_description' AS app_description,
        CASE
            WHEN NOT (data->'release_date'->>'coming_soon')::boolean
                AND data->'release_date'->>'date' ~ '^[A-Za-z]{3} \d{1,2}, \d{4}$'
            THEN to_date(data->'release_date'->>'date', 'Mon DD, YYYY')
        END AS release_date,
        (
            SELECT array_agg(g->>'description' ORDER BY ord)
            FROM jsonb_array_elements(data->'genres') WITH ORDINALITY AS t(g, ord)
        ) AS genre_names,
        CASE
            WHEN data->>'required_age' ~ '^\d+$'
            THEN (data->>'required_age')::int
        END AS required_age,
        (data->>'is_free')::boolean AS is_free,
        (data->'platforms'->>'windows')::boolean AS is_windows,
        (data->'platforms'->>'mac')::boolean AS is_mac,
        (data->'platforms'->>'linux')::boolean AS is_linux,
        (data->'metacritic'->>'score')::int AS metacritic_score,
        COALESCE((data->'achievements'->>'total')::int, 0) AS achievement_count,
        (data->'recommendations'->>'total')::int AS recommendation_count,
        data->'price_overview'->>'currency' AS price_currency,
        ((data->'price_overview'->>'initial')::int / 100.0)::numeric(10, 2) AS price_initial_usd,
        ((data->'price_overview'->>'final')::int / 100.0)::numeric(10, 2) AS price_final_usd
    FROM source
)

SELECT *
FROM renamed
