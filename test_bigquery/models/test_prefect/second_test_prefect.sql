SELECT
    species,
    AVG(year) AS avg_year
FROM
    {{ ref('test_prefect') }}
GROUP BY
    species