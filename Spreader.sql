CREATE SCHEMA IF NOT EXISTS sweetcoffeetree.coffeesales500m_v2_small;

CREATE TABLE sweetcoffeetree.coffeesales500m_v2_small.fact_sales
AS
SELECT *
FROM
sweetcoffeetree.coffeesales500m_v2_seed.fact_sales;

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
