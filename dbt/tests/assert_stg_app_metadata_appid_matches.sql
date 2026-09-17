SELECT *
FROM {{ ref('stg_app_metadata') }}
WHERE app_id != steam_appid
