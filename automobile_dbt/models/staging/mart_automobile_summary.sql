WITH base AS (
    SELECT * FROM {{ ref('stg_automobile') }}
),

enriched AS (
    SELECT
        make,
        fuel_type,
        body_style,
        drive_wheels,
        engine_size,
        horsepower,
        city_mpg,
        highway_mpg,
        price,

        -- price segmentation
        CASE
            WHEN price < 10000 THEN 'Budget'
            WHEN price < 20000 THEN 'Mid-Range'
            ELSE 'Premium'
        END AS price_tier,

        -- efficiency rating
        CASE
            WHEN city_mpg > 30 THEN 'High'
            WHEN city_mpg > 20 THEN 'Medium'
            ELSE 'Low'
        END AS efficiency_rating,

        -- performance score (simple index)
        ROUND(horsepower / NULLIF(price, 0) * 1000, 2) AS performance_per_dollar

    FROM base
)

SELECT * FROM enriched