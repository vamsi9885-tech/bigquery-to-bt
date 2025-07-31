from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# Start Spark session
spark = SparkSession.builder.appName("SCD_RealTimeTest").getOrCreate()

# --- Function to compute SHA column ---
def create_sha_field(df, column_name, included_columns=[], num_bits=256):
    filtered_type = df.select(*included_columns).dtypes
    return df.withColumn(column_name, sha2(concat_ws("|", *map(
        lambda col_dtypes: flatten(df[col_dtypes[0]]) if "array<array" in col_dtypes[1] else col_dtypes[0],
        filtered_type)), num_bits))

# --- Config ---
granularity_keys = ["customer_id"]
source = "ecomm"
target_db = "default"
target_table = "customer_scd"
active_ym = "202507"
dt_now = "2025-07-28 10:00:00"

# --- Realistic Day 0 History Data ---
data_day0 = [
    ("C001", "Alice", "alice@example.com", "Mumbai", 2500),
    ("C002", "Bob", "bob@example.com", "Delhi", 1800),
    ("C003", "Charlie", "charlie@example.com", "Bangalore", 3100),
    ("C004", "Diana", "diana@example.com", "Hyderabad", 1500),
    ("C005", "Ethan", "ethan@example.com", "Chennai", 2800),
]
columns = ["customer_id", "name", "email", "location", "last_purchase_amt"]
active_dt = "2025-07-28"

df_day0 = spark.createDataFrame(data_day0, columns)

final_day0 = (
    create_sha_field(df_day0, "rec_sha", df_day0.columns)
    .withColumn("active_dt", lit(active_dt))
    .withColumn("active_ym", lit(active_ym))
    .withColumn("load_dt", lit(dt_now))
    .withColumn("source_system_cd", lit(source))
    .withColumn("record_version_no", lit(1))
)

print("=== Day 0 - Initial History Load ===")
final_day0.show(truncate=False)

# Create initial history
history_df = final_day0

# --- Define Data for Days 1–5 ---
incremental_data = {
    1: [
        ("C001", "Alice", "alice@example.com", "Pune", 2700),  # Changed
        ("C006", "Frank", "frank@example.com", "Mumbai", 1900),  # New
        ("C002", "Bob", "bob@example.com", "Delhi", 1800),       # Unchanged
        ("C007", "Grace", "grace@example.com", "Kolkata", 2200), # New
    ],
    2: [
        ("C003", "Charlie", "charlie@example.com", "Bangalore", 3500),  # Changed
        ("C008", "Henry", "henry@example.com", "Ahmedabad", 2000),      # New
    ],
    3: [
        ("C001", "Alice", "alice@example.com", "Pune", 2700),
        ("C002", "Bob", "bob@example.com", "Delhi", 1800),
        ("C003", "Charlie", "charlie@example.com", "Bangalore", 3500),
        ("C004", "Diana", "diana@example.com", "Hyderabad", 1500),
    ],
    4: [
        ("C004", "Diana", "diana@example.com", "Pune", 1600),         # Changed
        ("C007", "Grace", "grace@example.com", "Kolkata", 2500),      # Changed
    ],
    5: [
        ("C009", "Isha", "isha@example.com", "Delhi", 3000),          # New
        ("C005", "Ethan", "ethan@example.com", "Chennai", 2800),      # Unchanged
    ],
}

# --- Run SCD Logic for Days 1–5 ---
for day in range(1, 6):
    print(f"running for the day {day}")
    active_dt = (datetime.strptime("2025-07-28", "%Y-%m-%d") + timedelta(days=day)).strftime("%Y-%m-%d")
    data_day = incremental_data[day]
    df_day = spark.createDataFrame(data_day, columns)

    # Add SHA and metadata
    final_load = (
        create_sha_field(df_day, "rec_sha", df_day.columns)
        .withColumn("active_dt", lit(active_dt))
        .withColumn("active_ym", lit(active_ym))
        .withColumn("load_dt", lit(dt_now))
        .withColumn("source_system_cd", lit(source))
    )
    print(f"=====incremental data frame for the day {day}========")
    final_load.show(truncate=False)

    # Prepare historical window
    w = Window.partitionBy([col(k) for k in granularity_keys]).orderBy(desc("active_dt"), desc("record_version_no"))

    history_data = (
        history_df
        .filter((col("source_system_cd") == source) & (col("active_dt") <= lit(active_dt)))
        .select(*granularity_keys, "rec_sha", "record_version_no", "active_dt")
        .withColumn("rnk", rank().over(w))
        .filter(col("rnk") == 1)
        .select(
            *[col(k).alias(f"src_{k}") for k in granularity_keys],
            col("rec_sha").alias("src_rec_sha"),
            col("record_version_no"),
        )
        .cache()
    )
    print(f"============history data on the day {day}==========")
    history_data.show(truncate=False)
    # Get changed or new records
    scd_data = final_load.alias("n").join(
        history_data.alias("h"),
        col("h.src_rec_sha") == col("n.rec_sha"),
        "left_anti"
    )
    print(f"============scd data on the day {day}==========")
    scd_data.show(truncate=False)

    join_keys = [col(f"h.src_{k}") == col(f"n.{k}") for k in granularity_keys]

    versioned_df = (
        scd_data.alias("n")
        .join(history_data.alias("h"), join_keys, "left_outer")
        .select(
            *[col("n." + c) for c in scd_data.columns] +
            [(coalesce(col("h.record_version_no"), lit(0)) + lit(1)).alias("record_version_no")]
        )
    )
    print(f"versioned df for the day {day}")
    versioned_df.show(truncate=False)

    # Update history
    history_df = history_df.unionByName(versioned_df)

    print(f"\n=== Day {day} - active_dt: {active_dt} ===")
    versioned_df.orderBy("customer_id").show(truncate=False)

# --- Final Output ---
print("\n=== Final SCD Table ===")
history_df.orderBy("customer_id", "record_version_no").show(100, truncate=False)




=== Day 0 - Initial History Load ===
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|customer_id|name   |email              |location |last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|record_version_no|
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|C001       |Alice  |alice@example.com  |Mumbai   |2500             |e88087dba22eedf8270d8636bd77144daf1ffd44972e676e391f8cc10cc37ef3|2025-07-28|202507   |2025-07-28 10:00:00|ecomm           |1                |
|C002       |Bob    |bob@example.com    |Delhi    |1800             |64c0d0aa64ce5b879dbffaffcaf79bf40e72a7ecffd2d17eacca83eabd86b8f4|2025-07-28|202507   |2025-07-28 10:00:00|ecomm           |1                |
|C003       |Charlie|charlie@example.com|Bangalore|3100             |b68090a69f59e2d0ddb8091ac2924f10a5d5786a51fbefddbf018c82a1eb4465|2025-07-28|202507   |2025-07-28 10:00:00|ecomm           |1                |
|C004       |Diana  |diana@example.com  |Hyderabad|1500             |bf03936bae12d9d66ba6e038d50ccd5392f7f49641e3f7649c4dbff0b13d9df8|2025-07-28|202507   |2025-07-28 10:00:00|ecomm           |1                |
|C005       |Ethan  |ethan@example.com  |Chennai  |2800             |f41c3902b3653b4cbeed8dfa460f687670df2ec270db595663efef5ef26055d3|2025-07-28|202507   |2025-07-28 10:00:00|ecomm           |1                |
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+

running for the day 1
=====incremental data frame for the day 1========
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+
|customer_id|name |email            |location|last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+
|C001       |Alice|alice@example.com|Pune    |2700             |f80fca6bbb3c966503eeae7b0946af79fcab30fc7d0236a6fdaa1bc12ef92ab3|2025-07-29|202507   |2025-07-28 10:00:00|ecomm           |
|C006       |Frank|frank@example.com|Mumbai  |1900             |d8e65d708b8bef0373e73ea3dc58eaac0cffe6d52ea0e46b699f9a8e30f69a8b|2025-07-29|202507   |2025-07-28 10:00:00|ecomm           |
|C002       |Bob  |bob@example.com  |Delhi   |1800             |64c0d0aa64ce5b879dbffaffcaf79bf40e72a7ecffd2d17eacca83eabd86b8f4|2025-07-29|202507   |2025-07-28 10:00:00|ecomm           |
|C007       |Grace|grace@example.com|Kolkata |2200             |94a2f89552b031a88bd149048d3acafb10d74734e2bdff4bbbf37332080b0ccd|2025-07-29|202507   |2025-07-28 10:00:00|ecomm           |
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+

============history data on the day 1==========
+---------------+----------------------------------------------------------------+-----------------+
|src_customer_id|src_rec_sha                                                     |record_version_no|
+---------------+----------------------------------------------------------------+-----------------+
|C003           |b68090a69f59e2d0ddb8091ac2924f10a5d5786a51fbefddbf018c82a1eb4465|1                |
|C004           |bf03936bae12d9d66ba6e038d50ccd5392f7f49641e3f7649c4dbff0b13d9df8|1                |
|C005           |f41c3902b3653b4cbeed8dfa460f687670df2ec270db595663efef5ef26055d3|1                |
|C001           |e88087dba22eedf8270d8636bd77144daf1ffd44972e676e391f8cc10cc37ef3|1                |
|C002           |64c0d0aa64ce5b879dbffaffcaf79bf40e72a7ecffd2d17eacca83eabd86b8f4|1                |
+---------------+----------------------------------------------------------------+-----------------+

============scd data on the day 1==========
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+
|customer_id|name |email            |location|last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+
|C001       |Alice|alice@example.com|Pune    |2700             |f80fca6bbb3c966503eeae7b0946af79fcab30fc7d0236a6fdaa1bc12ef92ab3|2025-07-29|202507   |2025-07-28 10:00:00|ecomm           |
|C006       |Frank|frank@example.com|Mumbai  |1900             |d8e65d708b8bef0373e73ea3dc58eaac0cffe6d52ea0e46b699f9a8e30f69a8b|2025-07-29|202507   |2025-07-28 10:00:00|ecomm           |
|C007       |Grace|grace@example.com|Kolkata |2200             |94a2f89552b031a88bd149048d3acafb10d74734e2bdff4bbbf37332080b0ccd|2025-07-29|202507   |2025-07-28 10:00:00|ecomm           |
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+

versioned df for the day 1
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|customer_id|name |email            |location|last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|record_version_no|
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|C001       |Alice|alice@example.com|Pune    |2700             |f80fca6bbb3c966503eeae7b0946af79fcab30fc7d0236a6fdaa1bc12ef92ab3|2025-07-29|202507   |2025-07-28 10:00:00|ecomm           |2                |
|C006       |Frank|frank@example.com|Mumbai  |1900             |d8e65d708b8bef0373e73ea3dc58eaac0cffe6d52ea0e46b699f9a8e30f69a8b|2025-07-29|202507   |2025-07-28 10:00:00|ecomm           |1                |
|C007       |Grace|grace@example.com|Kolkata |2200             |94a2f89552b031a88bd149048d3acafb10d74734e2bdff4bbbf37332080b0ccd|2025-07-29|202507   |2025-07-28 10:00:00|ecomm           |1                |
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+


=== Day 1 - active_dt: 2025-07-29 ===
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|customer_id|name |email            |location|last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|record_version_no|
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|C001       |Alice|alice@example.com|Pune    |2700             |f80fca6bbb3c966503eeae7b0946af79fcab30fc7d0236a6fdaa1bc12ef92ab3|2025-07-29|202507   |2025-07-28 10:00:00|ecomm           |2                |
|C006       |Frank|frank@example.com|Mumbai  |1900             |d8e65d708b8bef0373e73ea3dc58eaac0cffe6d52ea0e46b699f9a8e30f69a8b|2025-07-29|202507   |2025-07-28 10:00:00|ecomm           |1                |
|C007       |Grace|grace@example.com|Kolkata |2200             |94a2f89552b031a88bd149048d3acafb10d74734e2bdff4bbbf37332080b0ccd|2025-07-29|202507   |2025-07-28 10:00:00|ecomm           |1                |
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+

running for the day 2
=====incremental data frame for the day 2========
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+
|customer_id|name   |email              |location |last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+
|C003       |Charlie|charlie@example.com|Bangalore|3500             |feafcba8ce2b0830114221b85bda3b1f70aca09c3a2de017ededd29ce65e889a|2025-07-30|202507   |2025-07-28 10:00:00|ecomm           |
|C008       |Henry  |henry@example.com  |Ahmedabad|2000             |79f4eae50902d64c886b7009e9020b41d5c0d818e132a2b1863eedd44a1dd309|2025-07-30|202507   |2025-07-28 10:00:00|ecomm           |
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+

============history data on the day 2==========
+---------------+----------------------------------------------------------------+-----------------+
|src_customer_id|src_rec_sha                                                     |record_version_no|
+---------------+----------------------------------------------------------------+-----------------+
|C006           |d8e65d708b8bef0373e73ea3dc58eaac0cffe6d52ea0e46b699f9a8e30f69a8b|1                |
|C007           |94a2f89552b031a88bd149048d3acafb10d74734e2bdff4bbbf37332080b0ccd|1                |
|C003           |b68090a69f59e2d0ddb8091ac2924f10a5d5786a51fbefddbf018c82a1eb4465|1                |
|C004           |bf03936bae12d9d66ba6e038d50ccd5392f7f49641e3f7649c4dbff0b13d9df8|1                |
|C005           |f41c3902b3653b4cbeed8dfa460f687670df2ec270db595663efef5ef26055d3|1                |
|C001           |f80fca6bbb3c966503eeae7b0946af79fcab30fc7d0236a6fdaa1bc12ef92ab3|2                |
|C002           |64c0d0aa64ce5b879dbffaffcaf79bf40e72a7ecffd2d17eacca83eabd86b8f4|1                |
+---------------+----------------------------------------------------------------+-----------------+

============scd data on the day 2==========
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+
|customer_id|name   |email              |location |last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+
|C003       |Charlie|charlie@example.com|Bangalore|3500             |feafcba8ce2b0830114221b85bda3b1f70aca09c3a2de017ededd29ce65e889a|2025-07-30|202507   |2025-07-28 10:00:00|ecomm           |
|C008       |Henry  |henry@example.com  |Ahmedabad|2000             |79f4eae50902d64c886b7009e9020b41d5c0d818e132a2b1863eedd44a1dd309|2025-07-30|202507   |2025-07-28 10:00:00|ecomm           |
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+

versioned df for the day 2
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|customer_id|name   |email              |location |last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|record_version_no|
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|C003       |Charlie|charlie@example.com|Bangalore|3500             |feafcba8ce2b0830114221b85bda3b1f70aca09c3a2de017ededd29ce65e889a|2025-07-30|202507   |2025-07-28 10:00:00|ecomm           |2                |
|C008       |Henry  |henry@example.com  |Ahmedabad|2000             |79f4eae50902d64c886b7009e9020b41d5c0d818e132a2b1863eedd44a1dd309|2025-07-30|202507   |2025-07-28 10:00:00|ecomm           |1                |
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+


=== Day 2 - active_dt: 2025-07-30 ===
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|customer_id|name   |email              |location |last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|record_version_no|
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|C003       |Charlie|charlie@example.com|Bangalore|3500             |feafcba8ce2b0830114221b85bda3b1f70aca09c3a2de017ededd29ce65e889a|2025-07-30|202507   |2025-07-28 10:00:00|ecomm           |2                |
|C008       |Henry  |henry@example.com  |Ahmedabad|2000             |79f4eae50902d64c886b7009e9020b41d5c0d818e132a2b1863eedd44a1dd309|2025-07-30|202507   |2025-07-28 10:00:00|ecomm           |1                |
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+

running for the day 3
=====incremental data frame for the day 3========
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+
|customer_id|name   |email              |location |last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+
|C001       |Alice  |alice@example.com  |Pune     |2700             |f80fca6bbb3c966503eeae7b0946af79fcab30fc7d0236a6fdaa1bc12ef92ab3|2025-07-31|202507   |2025-07-28 10:00:00|ecomm           |
|C002       |Bob    |bob@example.com    |Delhi    |1800             |64c0d0aa64ce5b879dbffaffcaf79bf40e72a7ecffd2d17eacca83eabd86b8f4|2025-07-31|202507   |2025-07-28 10:00:00|ecomm           |
|C003       |Charlie|charlie@example.com|Bangalore|3500             |feafcba8ce2b0830114221b85bda3b1f70aca09c3a2de017ededd29ce65e889a|2025-07-31|202507   |2025-07-28 10:00:00|ecomm           |
|C004       |Diana  |diana@example.com  |Hyderabad|1500             |bf03936bae12d9d66ba6e038d50ccd5392f7f49641e3f7649c4dbff0b13d9df8|2025-07-31|202507   |2025-07-28 10:00:00|ecomm           |
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+

============history data on the day 3==========
+---------------+----------------------------------------------------------------+-----------------+
|src_customer_id|src_rec_sha                                                     |record_version_no|
+---------------+----------------------------------------------------------------+-----------------+
|C006           |d8e65d708b8bef0373e73ea3dc58eaac0cffe6d52ea0e46b699f9a8e30f69a8b|1                |
|C007           |94a2f89552b031a88bd149048d3acafb10d74734e2bdff4bbbf37332080b0ccd|1                |
|C003           |feafcba8ce2b0830114221b85bda3b1f70aca09c3a2de017ededd29ce65e889a|2                |
|C004           |bf03936bae12d9d66ba6e038d50ccd5392f7f49641e3f7649c4dbff0b13d9df8|1                |
|C008           |79f4eae50902d64c886b7009e9020b41d5c0d818e132a2b1863eedd44a1dd309|1                |
|C005           |f41c3902b3653b4cbeed8dfa460f687670df2ec270db595663efef5ef26055d3|1                |
|C001           |f80fca6bbb3c966503eeae7b0946af79fcab30fc7d0236a6fdaa1bc12ef92ab3|2                |
|C002           |64c0d0aa64ce5b879dbffaffcaf79bf40e72a7ecffd2d17eacca83eabd86b8f4|1                |
+---------------+----------------------------------------------------------------+-----------------+

============scd data on the day 3==========
+-----------+----+-----+--------+-----------------+-------+---------+---------+-------+----------------+
|customer_id|name|email|location|last_purchase_amt|rec_sha|active_dt|active_ym|load_dt|source_system_cd|
+-----------+----+-----+--------+-----------------+-------+---------+---------+-------+----------------+
+-----------+----+-----+--------+-----------------+-------+---------+---------+-------+----------------+

versioned df for the day 3
+-----------+----+-----+--------+-----------------+-------+---------+---------+-------+----------------+-----------------+
|customer_id|name|email|location|last_purchase_amt|rec_sha|active_dt|active_ym|load_dt|source_system_cd|record_version_no|
+-----------+----+-----+--------+-----------------+-------+---------+---------+-------+----------------+-----------------+
+-----------+----+-----+--------+-----------------+-------+---------+---------+-------+----------------+-----------------+


=== Day 3 - active_dt: 2025-07-31 ===
+-----------+----+-----+--------+-----------------+-------+---------+---------+-------+----------------+-----------------+
|customer_id|name|email|location|last_purchase_amt|rec_sha|active_dt|active_ym|load_dt|source_system_cd|record_version_no|
+-----------+----+-----+--------+-----------------+-------+---------+---------+-------+----------------+-----------------+
+-----------+----+-----+--------+-----------------+-------+---------+---------+-------+----------------+-----------------+

running for the day 4
=====incremental data frame for the day 4========
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+
|customer_id|name |email            |location|last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+
|C004       |Diana|diana@example.com|Pune    |1600             |8420495520c1e0c4cdbf310c263867bebd9ed963d62703898081ec9ee7b5a163|2025-08-01|202507   |2025-07-28 10:00:00|ecomm           |
|C007       |Grace|grace@example.com|Kolkata |2500             |f2a6578fc79a30f73c246da17556a8e2d42192d180225adeaa3e4e65a65718d7|2025-08-01|202507   |2025-07-28 10:00:00|ecomm           |
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+

============history data on the day 4==========
+---------------+----------------------------------------------------------------+-----------------+
|src_customer_id|src_rec_sha                                                     |record_version_no|
+---------------+----------------------------------------------------------------+-----------------+
|C006           |d8e65d708b8bef0373e73ea3dc58eaac0cffe6d52ea0e46b699f9a8e30f69a8b|1                |
|C007           |94a2f89552b031a88bd149048d3acafb10d74734e2bdff4bbbf37332080b0ccd|1                |
|C003           |feafcba8ce2b0830114221b85bda3b1f70aca09c3a2de017ededd29ce65e889a|2                |
|C004           |bf03936bae12d9d66ba6e038d50ccd5392f7f49641e3f7649c4dbff0b13d9df8|1                |
|C008           |79f4eae50902d64c886b7009e9020b41d5c0d818e132a2b1863eedd44a1dd309|1                |
|C005           |f41c3902b3653b4cbeed8dfa460f687670df2ec270db595663efef5ef26055d3|1                |
|C001           |f80fca6bbb3c966503eeae7b0946af79fcab30fc7d0236a6fdaa1bc12ef92ab3|2                |
|C002           |64c0d0aa64ce5b879dbffaffcaf79bf40e72a7ecffd2d17eacca83eabd86b8f4|1                |
+---------------+----------------------------------------------------------------+-----------------+

============scd data on the day 4==========
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+
|customer_id|name |email            |location|last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+
|C004       |Diana|diana@example.com|Pune    |1600             |8420495520c1e0c4cdbf310c263867bebd9ed963d62703898081ec9ee7b5a163|2025-08-01|202507   |2025-07-28 10:00:00|ecomm           |
|C007       |Grace|grace@example.com|Kolkata |2500             |f2a6578fc79a30f73c246da17556a8e2d42192d180225adeaa3e4e65a65718d7|2025-08-01|202507   |2025-07-28 10:00:00|ecomm           |
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+

versioned df for the day 4
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|customer_id|name |email            |location|last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|record_version_no|
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|C004       |Diana|diana@example.com|Pune    |1600             |8420495520c1e0c4cdbf310c263867bebd9ed963d62703898081ec9ee7b5a163|2025-08-01|202507   |2025-07-28 10:00:00|ecomm           |2                |
|C007       |Grace|grace@example.com|Kolkata |2500             |f2a6578fc79a30f73c246da17556a8e2d42192d180225adeaa3e4e65a65718d7|2025-08-01|202507   |2025-07-28 10:00:00|ecomm           |2                |
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+


=== Day 4 - active_dt: 2025-08-01 ===
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|customer_id|name |email            |location|last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|record_version_no|
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|C004       |Diana|diana@example.com|Pune    |1600             |8420495520c1e0c4cdbf310c263867bebd9ed963d62703898081ec9ee7b5a163|2025-08-01|202507   |2025-07-28 10:00:00|ecomm           |2                |
|C007       |Grace|grace@example.com|Kolkata |2500             |f2a6578fc79a30f73c246da17556a8e2d42192d180225adeaa3e4e65a65718d7|2025-08-01|202507   |2025-07-28 10:00:00|ecomm           |2                |
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+

running for the day 5
=====incremental data frame for the day 5========
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+
|customer_id|name |email            |location|last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+
|C009       |Isha |isha@example.com |Delhi   |3000             |425bc014a0e6774e585c7eb45cad143a4b0723b0a41b53c22a53aeb55ebd6cdb|2025-08-02|202507   |2025-07-28 10:00:00|ecomm           |
|C005       |Ethan|ethan@example.com|Chennai |2800             |f41c3902b3653b4cbeed8dfa460f687670df2ec270db595663efef5ef26055d3|2025-08-02|202507   |2025-07-28 10:00:00|ecomm           |
+-----------+-----+-----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+

============history data on the day 5==========
+---------------+----------------------------------------------------------------+-----------------+
|src_customer_id|src_rec_sha                                                     |record_version_no|
+---------------+----------------------------------------------------------------+-----------------+
|C006           |d8e65d708b8bef0373e73ea3dc58eaac0cffe6d52ea0e46b699f9a8e30f69a8b|1                |
|C007           |f2a6578fc79a30f73c246da17556a8e2d42192d180225adeaa3e4e65a65718d7|2                |
|C003           |feafcba8ce2b0830114221b85bda3b1f70aca09c3a2de017ededd29ce65e889a|2                |
|C004           |8420495520c1e0c4cdbf310c263867bebd9ed963d62703898081ec9ee7b5a163|2                |
|C008           |79f4eae50902d64c886b7009e9020b41d5c0d818e132a2b1863eedd44a1dd309|1                |
|C005           |f41c3902b3653b4cbeed8dfa460f687670df2ec270db595663efef5ef26055d3|1                |
|C001           |f80fca6bbb3c966503eeae7b0946af79fcab30fc7d0236a6fdaa1bc12ef92ab3|2                |
|C002           |64c0d0aa64ce5b879dbffaffcaf79bf40e72a7ecffd2d17eacca83eabd86b8f4|1                |
+---------------+----------------------------------------------------------------+-----------------+

============scd data on the day 5==========
+-----------+----+----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+
|customer_id|name|email           |location|last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|
+-----------+----+----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+
|C009       |Isha|isha@example.com|Delhi   |3000             |425bc014a0e6774e585c7eb45cad143a4b0723b0a41b53c22a53aeb55ebd6cdb|2025-08-02|202507   |2025-07-28 10:00:00|ecomm           |
+-----------+----+----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+

versioned df for the day 5
+-----------+----+----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|customer_id|name|email           |location|last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|record_version_no|
+-----------+----+----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|C009       |Isha|isha@example.com|Delhi   |3000             |425bc014a0e6774e585c7eb45cad143a4b0723b0a41b53c22a53aeb55ebd6cdb|2025-08-02|202507   |2025-07-28 10:00:00|ecomm           |1                |
+-----------+----+----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+


=== Day 5 - active_dt: 2025-08-02 ===
+-----------+----+----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|customer_id|name|email           |location|last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|record_version_no|
+-----------+----+----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|C009       |Isha|isha@example.com|Delhi   |3000             |425bc014a0e6774e585c7eb45cad143a4b0723b0a41b53c22a53aeb55ebd6cdb|2025-08-02|202507   |2025-07-28 10:00:00|ecomm           |1                |
+-----------+----+----------------+--------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+


=== Final SCD Table ===
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|customer_id|name   |email              |location |last_purchase_amt|rec_sha                                                         |active_dt |active_ym|load_dt            |source_system_cd|record_version_no|
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
|C001       |Alice  |alice@example.com  |Mumbai   |2500             |e88087dba22eedf8270d8636bd77144daf1ffd44972e676e391f8cc10cc37ef3|2025-07-28|202507   |2025-07-28 10:00:00|ecomm           |1                |
|C001       |Alice  |alice@example.com  |Pune     |2700             |f80fca6bbb3c966503eeae7b0946af79fcab30fc7d0236a6fdaa1bc12ef92ab3|2025-07-29|202507   |2025-07-28 10:00:00|ecomm           |2                |
|C002       |Bob    |bob@example.com    |Delhi    |1800             |64c0d0aa64ce5b879dbffaffcaf79bf40e72a7ecffd2d17eacca83eabd86b8f4|2025-07-28|202507   |2025-07-28 10:00:00|ecomm           |1                |
|C003       |Charlie|charlie@example.com|Bangalore|3100             |b68090a69f59e2d0ddb8091ac2924f10a5d5786a51fbefddbf018c82a1eb4465|2025-07-28|202507   |2025-07-28 10:00:00|ecomm           |1                |
|C003       |Charlie|charlie@example.com|Bangalore|3500             |feafcba8ce2b0830114221b85bda3b1f70aca09c3a2de017ededd29ce65e889a|2025-07-30|202507   |2025-07-28 10:00:00|ecomm           |2                |
|C004       |Diana  |diana@example.com  |Hyderabad|1500             |bf03936bae12d9d66ba6e038d50ccd5392f7f49641e3f7649c4dbff0b13d9df8|2025-07-28|202507   |2025-07-28 10:00:00|ecomm           |1                |
|C004       |Diana  |diana@example.com  |Pune     |1600             |8420495520c1e0c4cdbf310c263867bebd9ed963d62703898081ec9ee7b5a163|2025-08-01|202507   |2025-07-28 10:00:00|ecomm           |2                |
|C005       |Ethan  |ethan@example.com  |Chennai  |2800             |f41c3902b3653b4cbeed8dfa460f687670df2ec270db595663efef5ef26055d3|2025-07-28|202507   |2025-07-28 10:00:00|ecomm           |1                |
|C006       |Frank  |frank@example.com  |Mumbai   |1900             |d8e65d708b8bef0373e73ea3dc58eaac0cffe6d52ea0e46b699f9a8e30f69a8b|2025-07-29|202507   |2025-07-28 10:00:00|ecomm           |1                |
|C007       |Grace  |grace@example.com  |Kolkata  |2200             |94a2f89552b031a88bd149048d3acafb10d74734e2bdff4bbbf37332080b0ccd|2025-07-29|202507   |2025-07-28 10:00:00|ecomm           |1                |
|C007       |Grace  |grace@example.com  |Kolkata  |2500             |f2a6578fc79a30f73c246da17556a8e2d42192d180225adeaa3e4e65a65718d7|2025-08-01|202507   |2025-07-28 10:00:00|ecomm           |2                |
|C008       |Henry  |henry@example.com  |Ahmedabad|2000             |79f4eae50902d64c886b7009e9020b41d5c0d818e132a2b1863eedd44a1dd309|2025-07-30|202507   |2025-07-28 10:00:00|ecomm           |1                |
|C009       |Isha   |isha@example.com   |Delhi    |3000             |425bc014a0e6774e585c7eb45cad143a4b0723b0a41b53c22a53aeb55ebd6cdb|2025-08-02|202507   |2025-07-28 10:00:00|ecomm           |1                |
+-----------+-------+-------------------+---------+-----------------+----------------------------------------------------------------+----------+---------+-------------------+----------------+-----------------+
