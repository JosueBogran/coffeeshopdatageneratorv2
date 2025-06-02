from pyspark.sql import SparkSession, functions as F, types as T
from datetime import datetime, timedelta
import math

# -------------------- Parameters --------------------
schema_name        = "coffeesales5b_v2_seed"
fact_table_name    = "fact_sales"
from_date_str      = "2023-01-01"
to_date_str        = "2024-12-31"
total_order_count  = 5_000_000_000

# Relative growth weights per “YYYY-MM”
relative_growth_weights = {
    "2023-01": 1.00, "2023-02": 0.90, "2023-03": 0.95, "2023-04": 1.05,
    "2023-05": 1.12, "2023-06": 1.05, "2023-07": 1.00, "2023-08": 1.15,
    "2023-09": 1.17, "2023-10": 1.09, "2023-11": 1.25, "2023-12": 1.22,
    "2024-01": 1.04, "2024-02": 1.32, "2024-03": 1.28, "2024-04": 1.13,
    "2024-05": 1.35, "2024-06": 1.40, "2024-07": 1.42, "2024-08": 1.48,
    "2024-09": 1.50, "2024-10": 1.37, "2024-11": 1.45, "2024-12": 1.60,
}

# Relative region weights by month (user can adjust per YYYY-MM and region)
relative_region_weights = {
    "2023-01": {"Southeast": 0.90, "South": 1.00, "West": 1.30},
    "2023-02": {"Southeast": 0.90, "South": 1.00, "West": 1.25},
    "2023-03": {"Southeast": 0.95, "South": 1.00, "West": 1.15},
    "2023-04": {"Southeast": 1.00, "South": 1.00, "West": 1.05},
    "2023-05": {"Southeast": 1.05, "South": 1.00, "West": 1.00},
    "2023-06": {"Southeast": 1.10, "South": 0.90, "West": 0.95},
    "2023-07": {"Southeast": 1.15, "South": 0.85, "West": 0.90},
    "2023-08": {"Southeast": 1.15, "South": 0.85, "West": 0.90},
    "2023-09": {"Southeast": 1.05, "South": 1.00, "West": 1.00},
    "2023-10": {"Southeast": 1.00, "South": 1.00, "West": 1.10},
    "2023-11": {"Southeast": 0.95, "South": 1.00, "West": 1.15},
    "2023-12": {"Southeast": 0.90, "South": 1.00, "West": 1.25},

    "2024-01": {"Southeast": 0.90, "South": 1.00, "West": 1.30},
    "2024-02": {"Southeast": 0.90, "South": 1.00, "West": 1.25},
    "2024-03": {"Southeast": 0.95, "South": 1.00, "West": 1.15},
    "2024-04": {"Southeast": 1.00, "South": 1.00, "West": 1.05},
    "2024-05": {"Southeast": 1.05, "South": 1.00, "West": 1.00},
    "2024-06": {"Southeast": 1.10, "South": 0.90, "West": 0.95},
    "2024-07": {"Southeast": 1.15, "South": 0.85, "West": 0.90},
    "2024-08": {"Southeast": 1.15, "South": 0.85, "West": 0.90},
    "2024-09": {"Southeast": 1.05, "South": 1.00, "West": 1.00},
    "2024-10": {"Southeast": 1.00, "South": 1.00, "West": 1.10},
    "2024-11": {"Southeast": 0.95, "South": 1.00, "West": 1.15},
    "2024-12": {"Southeast": 0.90, "South": 1.00, "West": 1.25},
}

PCT_STORES_POSITIVE = 0.3
PCT_STORES_FLAT     = 0.5
PCT_STORES_NEGATIVE = 0.2

RAND_SEED_BASE      = 42
RAND_SEED_TREND     = 99
RAND_SEED_REGION    = 123
RAND_SEED_TIMEOFDAY = 2025


spark.sql(f"CREATE SCHEMA IF NOT EXISTS sweetcoffeetree.{schema_name}")

# -------------------- Build Month Weights DataFrame --------------------
month_weights_rows = [
    (int(ym.split("-")[0]), int(ym.split("-")[1]), w)
    for ym, w in relative_growth_weights.items()
]
month_weights_schema = T.StructType([
    T.StructField("year",  T.IntegerType(), False),
    T.StructField("month", T.IntegerType(), False),
    T.StructField("weight",T.DoubleType(),  False),
])
month_weights_df = spark.createDataFrame(month_weights_rows, schema=month_weights_schema)

total_weight = sum(relative_growth_weights.values())
month_weights_df = month_weights_df.withColumn(
    "monthly_orders",
    F.expr(f"{float(total_order_count)} * (weight / {float(total_weight)})").cast("long")
).drop("weight")

# -------------------- Build Region Weights DataFrame --------------------
rows = []
for ym, region_map in relative_region_weights.items():
    y, mo = map(int, ym.split("-"))
    for region_name, w in region_map.items():
        rows.append((y, mo, region_name, float(w)))
region_wts_schema = T.StructType([
    T.StructField("year",          T.IntegerType(), False),
    T.StructField("month",         T.IntegerType(), False),
    T.StructField("region",        T.StringType(),  False),
    T.StructField("region_weight", T.DoubleType(),  False),
])
region_weights_df = spark.createDataFrame(rows, schema=region_wts_schema)

# -------------------- Assign Trends & Base Traffic to Locations --------------------
locations_df = spark.table(f"sweetcoffeetree.{schema_name}.dim_locations").select("location_id")

locations_with_rand = locations_df.withColumn("rand_val", F.rand(RAND_SEED_TREND))
locations_trend = locations_with_rand.withColumn(
    "trend_type",
    F.when(F.col("rand_val") < float(PCT_STORES_POSITIVE),   F.lit("positive"))
     .when(F.col("rand_val") < float(PCT_STORES_POSITIVE + PCT_STORES_FLAT), F.lit("flat"))
     .otherwise(F.lit("negative"))
).drop("rand_val")

locations_trend = locations_trend.withColumn(
    "base_traffic",
    (F.rand(RAND_SEED_BASE) * 0.6 + 0.7).cast("double")
)

# Assign a “region” label per location (user’s dim_locations must have a region field or adjust logic)
locations_trend = locations_trend.withColumn(
    "region",
    F.when(F.rand(RAND_SEED_REGION) < 0.25, F.lit("Southeast"))
     .when(F.rand(RAND_SEED_REGION) < 0.50, F.lit("South"))
     .when(F.rand(RAND_SEED_REGION) < 0.75, F.lit("West"))
     .otherwise(F.lit("North"))
)

# -------------------- Helper: Generate quarter ranges --------------------
def generate_quarters(start_str, end_str):
    quarters = []
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date   = datetime.strptime(end_str,   "%Y-%m-%d")
    year, month = start_date.year, start_date.month
    cur_q_start = datetime(year, ((month - 1) // 3) * 3 + 1, 1)
    if cur_q_start > start_date:
        cur_q_start = datetime(year, ((month - 1) // 3) * 3 + 1, 1)
    while cur_q_start <= end_date:
        next_q_start = (cur_q_start.replace(day=1) + timedelta(days=92)).replace(day=1)
        quarter_end = next_q_start - timedelta(days=1)
        if quarter_end > end_date:
            quarter_end = end_date
        actual_start = max(cur_q_start, start_date)
        actual_end   = quarter_end
        if actual_start <= actual_end:
            quarters.append((actual_start.strftime("%Y-%m-%d"), actual_end.strftime("%Y-%m-%d")))
        cur_q_start = next_q_start
    return quarters

quarter_ranges = generate_quarters(from_date_str, to_date_str)

# -------------------- Main Loop: Process Each Quarter Separately --------------------
for (chunk_start, chunk_end) in quarter_ranges:
    # Build calendar for this quarter
    calendar_base = (
        spark
        .range(1)
        .select(
            F.expr(f"sequence(to_date('{chunk_start}'), to_date('{chunk_end}'), interval 1 day) as all_dates")
        )
    )

    calendar_df = (
        calendar_base
        .select(F.explode("all_dates").alias("order_date"))
        .withColumn("year",      F.year("order_date"))
        .withColumn("month",     F.month("order_date"))
        .withColumn("day_of_year", F.dayofyear("order_date"))
        .withColumn("day_of_week", F.dayofweek("order_date"))
        .withColumn(
            "seasonality",
            F.round(1 + 0.2 * F.sin(F.col("day_of_year") * 2 * math.pi / 365), 4)
        )
    )

    dow_raw = {1:1.0, 2:1.0, 3:1.0, 4:1.05, 5:1.15, 6:1.25, 7:1.2}
    raw_total = sum(dow_raw.values())
    normalized_dow = {k: round((v / raw_total) * 7.0, 4) for k,v in dow_raw.items()}

    dow_map_entries = []
    for k, v in normalized_dow.items():
        dow_map_entries.append(F.lit(k))
        dow_map_entries.append(F.lit(v))
    dow_map = F.create_map(*dow_map_entries)

    calendar_df = calendar_df.withColumn(
        "dow_multiplier",
        dow_map[F.col("day_of_week")]
    )

    # Compute traffic multipliers for this quarter
    traffic_df = (
        locations_trend.crossJoin(calendar_df)
        .withColumn(
            "trend_multiplier",
            F.when(F.col("trend_type") == "positive", 1 + 0.0008 * F.col("day_of_year"))
             .when(F.col("trend_type") == "negative", 1 - 0.0008 * F.col("day_of_year"))
             .otherwise(F.lit(1.0))
        )
        .withColumn("noise", (F.rand(RAND_SEED_BASE + 1) * 0.04 + 0.98))
        .withColumn(
            "base_calc",
            F.col("base_traffic")
            * F.col("trend_multiplier")
            * F.col("seasonality")
            * F.col("noise")
            * F.col("dow_multiplier")
        )
        .select("location_id", "region", "year", "month", "order_date", "base_calc")
    )

    # Join region weights and apply them
    traffic_with_region = (
        traffic_df
        .join(region_weights_df, on=["year", "month", "region"], how="left")
        .withColumn("region_weight", F.coalesce(F.col("region_weight"), F.lit(1.0)))
        .withColumn(
            "traffic_multiplier",
            F.round(F.col("base_calc") * F.col("region_weight"), 4)
        )
        .select("location_id", "region", "year", "month", "order_date", "traffic_multiplier")
    )

    # Join monthly_orders
    traffic_with_monthly = traffic_with_region.join(
        month_weights_df, on=["year", "month"], how="left"
    )

    monthly_multiplier_sum = (
        traffic_with_monthly
        .groupBy("year", "month")
        .agg(F.sum("traffic_multiplier").alias("monthly_total_multiplier"))
    )

    traffic_with_norm = (
        traffic_with_monthly
        .join(monthly_multiplier_sum, on=["year", "month"], how="left")
        .withColumn("orders_per_unit", (F.col("monthly_orders") / F.col("monthly_total_multiplier")))
        .withColumn("order_count", F.round(F.col("orders_per_unit") * F.col("traffic_multiplier")).cast("int"))
        .filter(F.col("order_count") > 0)
        .select("order_date", "location_id", "region", "month", "order_count")
    )

    # Explode into individual orders
    orders_df = (
        traffic_with_norm
        .withColumn("order_array", F.expr("sequence(1, order_count)"))
        .select(
            "order_date", "location_id", "region", "month",
            F.posexplode("order_array").alias("order_idx", "_dummy")
        )
        .drop("_dummy")
        .withColumn(
            "order_id",
            F.sha2(
                F.concat_ws("_", F.col("order_date").cast("string"), F.col("location_id"), F.col("order_idx").cast("string")),
                256
            )
        )
        .select("order_id", "order_date", "location_id", "region", "month")
    )

    # Assign time_of_day and number of lines per order
    orders_df = (
        orders_df
        .withColumn(
            "time_of_day",
            F.when(F.rand(RAND_SEED_TIMEOFDAY) < 0.5, F.lit("Morning"))
             .when(F.rand(RAND_SEED_TIMEOFDAY + 1) < 0.8, F.lit("Afternoon"))
             .otherwise(F.lit("Night"))
        )
        .withColumn(
            "num_lines",
            F.when(F.rand(RAND_SEED_TIMEOFDAY + 2) < 0.6, 1)
             .when(F.rand(RAND_SEED_TIMEOFDAY + 3) < 0.9, 2)
             .when(F.rand(RAND_SEED_TIMEOFDAY + 4) < 0.95,3)
             .when(F.rand(RAND_SEED_TIMEOFDAY + 5) < 0.96,4)
             .otherwise(5)
             .cast("int")
        )
    )

    # Explode into order lines
    order_lines_df = (
        orders_df
        .withColumn("line_array", F.expr("sequence(1, num_lines)"))
        .select(
            "order_id", "order_date", "location_id", "region", "month", "time_of_day",
            F.posexplode("line_array").alias("line_idx", "line_val")
        )
        .withColumn(
            "order_line_id",
            F.concat(F.col("order_id"), F.lit("_"), F.col("line_val"))
        )
        .drop("line_array", "line_val")
    )

    # Assign product_id, quantity, discount, season
    summer_products     = F.array(*[F.lit(x) for x in [5, 6]])
    non_summer_products = F.array(*[F.lit(x) for x in [1, 2, 3, 4, 7, 8, 9, 10]])
    other_products      = F.array(*[F.lit(x) for x in [11, 12, 13]])

    final_line_items = (
        order_lines_df
        .withColumn(
            "season",
            F.when(F.col("month").isin([12, 1, 2]), F.lit("winter"))
             .when(F.col("month").isin([3, 4, 5]),  F.lit("spring"))
             .when(F.col("month").isin([6, 7, 8]),  F.lit("summer"))
             .otherwise(F.lit("fall"))
        )
        .withColumn("rand_val", F.rand(RAND_SEED_BASE + 10))
        .withColumn(
            "product_id",
            F.when(F.col("season") == "summer",
                F.when(F.col("rand_val") < 0.4,
                    F.element_at(summer_products, (F.floor(F.rand(RAND_SEED_BASE+11) * 2) + 1).cast("int"))
                )
                .when(F.col("rand_val") < 0.9,
                    F.element_at(non_summer_products, (F.floor(F.rand(RAND_SEED_BASE+12) * 8) + 1).cast("int"))
                )
                .otherwise(
                    F.element_at(other_products, (F.floor(F.rand(RAND_SEED_BASE+13) * 3) + 1).cast("int"))
                )
            ).otherwise(
                F.when(F.col("rand_val") < 0.7,
                    F.element_at(non_summer_products, (F.floor(F.rand(RAND_SEED_BASE+14) * 8) + 1).cast("int"))
                )
                .when(F.col("rand_val") < 0.8,
                    F.element_at(summer_products, (F.floor(F.rand(RAND_SEED_BASE+15) * 2) + 1).cast("int"))
                )
                .otherwise(
                    F.element_at(other_products, (F.floor(F.rand(RAND_SEED_BASE+16) * 3) + 1).cast("int"))
                )
            )
        )
        .withColumn(
            "quantity",
            F.when(F.rand(RAND_SEED_BASE+20) < 0.4, 1)
             .when(F.rand(RAND_SEED_BASE+21) < 0.7, 2)
             .when(F.rand(RAND_SEED_BASE+22) < 0.85,3)
             .when(F.rand(RAND_SEED_BASE+23) < 0.95,4)
             .otherwise(5)
             .cast("int")
        )
        .withColumn(
            "discount_rate",
            F.when(F.rand(RAND_SEED_BASE+30) < 0.8, 0)
             .otherwise((F.floor(F.rand(RAND_SEED_BASE+31) * 15) + 1).cast("int"))
        )
        .drop("rand_val")
    )

    # Persist this quarter’s factsbase in append mode
    (
        final_line_items
        .select(
            "order_id", "order_line_id", "order_date", "time_of_day", "season",
            "month", "location_id", "region", "product_id", "quantity", "discount_rate"
        )
        .write
        .format("delta")
        .mode("append")
        .option("overwriteSchema", "true")
        .saveAsTable(f"sweetcoffeetree.{schema_name}.factsbase")
    )

# Once all quarters are done, build/overwrite the final fact_sales table
spark.sql(f"""
CREATE OR REPLACE TABLE sweetcoffeetree.{schema_name}.{fact_table_name} AS
SELECT
  f.order_id,
  f.order_line_id,
  f.order_date,
  f.time_of_day,
  f.season,
  f.month,
  f.location_id,
  f.region,
  p.name          AS product_name,
  f.quantity,
  ROUND(p.standard_price * ((100 - f.discount_rate) / 100) * f.quantity, 2) AS sales_amount,
  f.discount_rate AS discount_percentage,
  f.product_id
FROM sweetcoffeetree.{schema_name}.factsbase AS f
LEFT JOIN sweetcoffeetree.{schema_name}.dim_products AS p
  ON f.product_id = p.product_id
  AND f.order_date BETWEEN p.from_date AND p.to_date
""")
