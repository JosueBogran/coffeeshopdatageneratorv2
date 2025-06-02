# coffeeshopdatageneratorv2
Generate dummy data with a coffee-shop theme. Lots of room for improvement, but functional and customizable!

These are the steps neccesary to generate the data exactly the way I did it for my published performance comparisons. You may be able to skip-as needed.

1.	Created sweetcoffeetree database directly on Databricks.
2.	Created sweetcoffeetree.coffeesales500m_v2_seed schema. 
   - The 500m represents an order count of 500m on the settings which will create an event greater amount of rows. 
   - The “seed” represents that this is the starting schema for our data generation.
3.	Uploaded the dim_locations table using the "Data Ingestion" UI.
   - All fields in dim_locations were set to STRING format.
4.  Uploaded the dim_products table using the "Data Ingestion" UI.
   - record_id, product_id, name, category, and subcategory were to STRING.
   - standard_cost and standard_price are set to DOUBLE.
   - from_date and to_date are set to DATE.
5.	Ran “Data Generator” using Serverless compute to generate the data with the relevant schema name and order count.
   - There is room for adjusting seeds for randomness, weight orders towards a month or another, and a few other knobs. Keeping things exactly as I ran them on this upload.
6.	Ran “Quick Visual” SQL script to test that everything looked good on the seed schema data.
7.	Ran “Spreader” script, in which we copy the facts, location, and product tables to a schema specific to the warehouse size we are testing.
8.	Created SQL Serverless Warehouse, for example, “Small_500m", Small_"1b", "large_5b", etc.
   - Test results are only used when all of the following are true: It is the first time a specific warehouse, with the query being used, to the matching schema/table combination.
9.	Ran a “SELECT ‘1’” as a warm up using the warehouse I tested with the corresponding queries.
10.	Ran Query_01 through Query_17, after each previous query completed inside the SQL Editor/Snowflake equivalent. Made sure there was no row limit on results.
11.	Repeated relevant steps for other sized/data size combination.
12.	Copied data inside the “Seed” schema data into snowflake via Spark connector, and repeated 7-10 inside of Snowflake.
