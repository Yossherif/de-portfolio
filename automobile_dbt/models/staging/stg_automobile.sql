WITH source AS (
    SELECT * FROM {{ source('automobile', 'automobile') }}
),

cleaned AS (
    SELECT
        -- identifiers
        make,
        `fuel-type`         AS fuel_type,
        aspiration,
        `body-style`        AS body_style,
        `drive-wheels`      AS drive_wheels,
        `engine-location`   AS engine_location,
        `num-of-doors`      AS num_of_doors,

        -- numerics (cast properly)
        SAFE_CAST(`wheel-base` AS FLOAT64)          AS wheel_base,
        SAFE_CAST(`engine-size` AS INT64)           AS engine_size,
        SAFE_CAST(horsepower AS INT64)              AS horsepower,
        SAFE_CAST(`city-mpg` AS INT64)              AS city_mpg,
        SAFE_CAST(`highway-mpg` AS INT64)           AS highway_mpg,
        SAFE_CAST(price AS FLOAT64)                 AS price,

        -- nullify missing values
        NULLIF(`normalized-losses`, '?')            AS normalized_losses

    FROM source
    WHERE price != '?'
)

SELECT * FROM cleaned