--Note: Adjust accordingly to the test requirements that you have.

CREATE SCHEMA IF NOT EXISTS sweetcoffeetree.coffeesales500m_v2_small;

CREATE TABLE sweetcoffeetree.coffeesales500m_v2_small.fact_sales
CLUSTER BY (order_date, product_name, location_id)
AS
SELECT *
FROM
sweetcoffeetree.coffeesales500m_v2_seed.fact_sales
ORDER BY order_date ASC, product_name ASC, location_id ASC;
;

CREATE TABLE sweetcoffeetree.coffeesales500m_v2_small.dim_products
AS
SELECT *
FROM
sweetcoffeetree.coffeesales500m_v2_seed.dim_products;

CREATE TABLE sweetcoffeetree.coffeesales500m_v2_small.dim_locations
AS
SELECT *
FROM
sweetcoffeetree.coffeesales500m_v2_seed.dim_locations;
