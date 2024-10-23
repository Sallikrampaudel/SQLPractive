from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

def healthcare():
    # Initialize Spark Session
    spark = SparkSession.builder.appName("HealthcareDataProcessing").enableHiveSupport().getOrCreate()
    # Read Patient Data from HDFS
    patient_df = spark.read.csv("hdfs:///data/test/patient_data.txt", header=True, inferSchema=True)
    # Read Claims Data from HDFS
    claims_df = spark.read.csv("hdfs:///data/test/clams_data.txt", header=True, inferSchema=True)


    # Data Cleaning: Remove Duplicates
    patient_df = patient_df.dropDuplicates()
    claims_df = claims_df.dropDuplicates()

    # Handle Null Values: Replace NULLs with Default Values
    patient_df = patient_df.fillna({"first_name": "Unknown", "address": "Unknown", "insurance_plan_id": -1})
    claims_df = claims_df.fillna({"claim_amount": 0.0})


    patient_df.write.mode("overwrite").saveAsTable("healthcare_db.cleaned_patient_data")
    claims_df.write.mode("overwrite").saveAsTable("healthcare_db.cleaned_claims_data")

    # Check Record Counts Between Source and Target
    source_patient_count = patient_df.count()
    target_patient_count = spark.sql("SELECT COUNT(*) FROM healthcare_db.cleaned_patient_data").collect()[0][0]

    if source_patient_count == target_patient_count:
        print("Patient data is complete and consistent.")
    else:
        print("Data inconsistency detected in patient records.")

    source_claims_count = claims_df.count()
    target_claims_count = spark.sql("SELECT COUNT(*) FROM healthcare_db.cleaned_claims_data").collect()[0][0]

    if source_claims_count == target_claims_count:
        print("Claims data is complete and consistent.")
    else:
        print("Data inconsistency detected in claims records.")


if __name__ == '__main__':
    healthcare()