from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, trim, lower


def customer_data_integration_api():
    # Initialize Spark Session
    spark = SparkSession.builder.appName("EcommerceDataProcessing").enableHiveSupport().getOrCreate()
    customer_df = spark.read.csv("hdfs:///user/test/ecomm/customer_data.txt", header=True, inferSchema=True)
    # Read Sales Transactions Data from HDFS
    sales_df = spark.read.csv("hdfs:///user/test/ecomm/sales_data.csv", header=True, inferSchema=True)

    # Data Cleaning: Remove Duplicates
    customer_df = customer_df.dropDuplicates()
    sales_df = sales_df.dropDuplicates()


    # Handle Null Values: Replace NULLs with Default Values
    customer_df = customer_df.fillna(
        {"first_name": "Unknown", "phone": "Unknown", "address": "Unknown", "total_spent": 0.0})
    sales_df = sales_df.fillna({"quantity": 0, "price": 0.0})


    # Standardize Data Formats: Trim Whitespaces and Convert to Lowercase
    customer_df = customer_df.withColumn("email", trim(lower(col("email"))))
    sales_df = sales_df.withColumn("payment_method", trim(lower(col("payment_method"))))

    # Write Cleaned Data to Hive Tables
    customer_df.write.mode("overwrite").saveAsTable("ecommerce_db.cleaned_customer_data")
    sales_df.write.mode("overwrite").saveAsTable("ecommerce_db.cleaned_sales_data")

    # Check Record Counts Between Source and Target
    source_customer_count = customer_df.count()
    target_customer_count = spark.sql("SELECT COUNT(*) FROM ecommerce_db.cleaned_customer_data").collect()[0][0]

    if source_customer_count == target_customer_count:
        print("Customer data is complete and consistent.")
    else:
        print("Data inconsistency detected in customer records.")

    source_sales_count = sales_df.count()
    target_sales_count = spark.sql("SELECT COUNT(*) FROM ecommerce_db.cleaned_sales_data").collect()[0][0]

    if source_sales_count == target_sales_count:
        print("Sales data is complete and consistent.")
    else:
        print("Data inconsistency detected in sales records.")

if __name__ == '__main__':
    customer_data_integration_api()