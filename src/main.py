from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, when

# Create Spark Session
spark = (
    SparkSession.builder
    .appName("DataQuality&ValidationPipeline")
    .master("local[*]")
    .getOrCreate()
)


# ==== EXTRACT ====


# Read the Dataset
orders_df = (
    spark.read.csv(
        "data/orders.csv",
        header=True,
        inferSchema=True
    )
)

# Display the Dataset
print("\n--- Orders Dataset ----")
orders_df.show()


# ==== TRANSFORM ====


# Convert Order Date from String to Date
orders_date_df = (
    orders_df.withColumn(
        "order_date",
        to_date("order_date")
    )
)

# Display the Schema of Converted Dataset
print("\n--- Orders Dataset Schema ---")
orders_date_df.printSchema()

# Create Validation
order_validation_df = (
    orders_date_df
    .withColumn(
        "is_valid",
        when(
            (orders_date_df["order_id"].isNotNull()) &
            (orders_date_df["customer_id"].isNotNull()) &
            (orders_date_df["amount"].isNotNull()) &
            (orders_date_df["amount"] > 0) &
            (orders_date_df["order_date"].isNotNull()),
            True
        ).otherwise(False)
    )
)

# Display The Validation
print("\n--- Order Dataset with Validation ---")
order_validation_df.show()

# Create Validation Status
orders_with_validation_status_df = (
    order_validation_df
    .withColumn(
        "validation_status",
        when(order_validation_df["order_id"].isNull(), "INVALID_ORDER")
        .when(order_validation_df["customer_id"].isNull(), "INVALID_CUSTOMER")
        .when(order_validation_df["amount"].isNull(), "MISSING_AMOUNT")
        .when(order_validation_df["amount"] <= 0, "INVALID_AMOUNT")
        .when(order_validation_df["order_date"].isNull(), "MISSING_DATE")
        .otherwise("VALID")
    )
)

# Display Validation Status
print("\n--- Orders With Validation Status ---")
orders_with_validation_status_df.show()

# Seperate Valid Orders with Invalid Orders
valid_order_df = (
    orders_with_validation_status_df
    .filter(
        orders_with_validation_status_df["is_valid"] == True 
    )
)

invalid_order_df = (
    orders_with_validation_status_df
    .filter(
        orders_with_validation_status_df["is_valid"] == False
    )
)

# Display Valid and Invalid Order Dataset
print("\n--- Valid Order Dataset ---")
valid_order_df.show()

print("\n--- Invalid Order Dataset ---")
invalid_order_df.show()

# Create Validation Summary
validation_summary_df =(
    orders_with_validation_status_df
    .groupBy(
        "validation_status"
    )
    .count()
    .orderBy(
        "count",
        ascending=False
    )
)

# Display Validation Summary
print("\n--- Validation Summary ---")
validation_summary_df.show()

total_records_count = orders_with_validation_status_df.count()
valid_records_count = valid_order_df.count()
invalid_records_count = invalid_order_df.count()
data_quality = valid_records_count / total_records_count * 100

print("\n" + "=" * 40)
print("Data Quality Validation Completed")
print("=" * 40)
print(f"\nTotal Records   : {total_records_count}")
print(f"Valid Records   : {valid_records_count}")
print(f"Invalid Records : {invalid_records_count}")
print(f"Data Quality%   : {data_quality:.2f} %")


# ==== LOAD ====


valid_order_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/valid_orders/")

print("\nValid Orders Dataset Saved Successfully.")

invalid_order_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/invalid_orders/")

print("\nInvalid Orders Dataset Saved Successfully.")

spark.stop()