WITH base AS (
    SELECT * FROM {{ ref('mart_automobile_summary') }}
)

SELECT
    make,
    COUNT(*)                            AS total_models,
    ROUND(AVG(price), 2)               AS avg_price,
    ROUND(AVG(horsepower), 1)          AS avg_horsepower,
    ROUND(AVG(city_mpg), 1)            AS avg_city_mpg,
    MIN(price)                         AS min_price,
    MAX(price)                         AS max_price,
    -- most common body style per make
    APPROX_TOP_COUNT(body_style, 1)[OFFSET(0)].value AS most_common_body_style,
    -- most common price tier per make
    APPROX_TOP_COUNT(price_tier, 1)[OFFSET(0)].value AS dominant_price_tier

FROM base
GROUP BY make
ORDER BY avg_price DESC