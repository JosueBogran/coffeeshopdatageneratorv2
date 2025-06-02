--This is not neccesary, but a good sanity check.
SELECT
  sales.order_date,
  products.name AS product_name,
  locations.city,
  sales.time_of_day,
  products.category AS product_category,
  SUM(sales.quantity) AS quantity,
  SUM(sales.sales_amount) AS revenue,
  COUNT(*) AS row_count
FROM sweetcoffeetree.coffeesales500m_v2_seed.fact_sales sales
JOIN sweetcoffeetree.coffeesales500m_v2_seed.dim_locations locations
  ON sales.location_id = locations.location_id
JOIN sweetcoffeetree.coffeesales500m_v2_seed.dim_products products
  ON sales.product_id = products.product_id
  AND sales.order_date BETWEEN products.from_date AND products.to_date
WHERE sales.order_date BETWEEN '2023-01-01' AND '2024-12-31'
GROUP BY ALL
