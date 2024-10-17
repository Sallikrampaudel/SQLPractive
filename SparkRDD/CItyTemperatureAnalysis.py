"""
City Temperature Analysis
Problem Statement
●	As a data engineer you are supposed to prepare the data from temp analysis
●	Your pipeline should return data in form of following columns
○	city
○	avg_temperature
○	total_temperature
○	num_measurements
●	You should return metrics for only those cities when total_temperature is greater than 30
●	And output should be sorted on city in ascending order

Data

 New York , 10.0
 New York , 12.0
 Los Angeles , 20.0
 Los Angeles , 22.0
 San Francisco , 15.0
 San Francisco , 18.0
Metadata- columns

city - String
temperature - Double
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, count

def city_temperature():
    # Initialize Spark session
    spark = SparkSession.builder.appName("City Data Processing").getOrCreate()

    # Define the sample data
    data = [
        ("New York", 10.0),
        ("New York", 12.0),
        ("Los Angeles", 20.0),
        ("Los Angeles", 22.0),
        ("San Francisco", 15.0),
        ("San Francisco", 18.0)
    ]

    # Define the column names
    columns = ["City", "temperature"]

    # Step 1: Create a DataFrame
    df = spark.createDataFrame(data, columns)
    df_new = df.groupBy("City").agg(
        avg("temperature").alias("avg_temperature"),
        sum("temperature").alias("total_temperature"),
        count("temperature").alias("num_measurements")
    )
    # Step 5: Filter cities where total_temperature > 30
    df_filtered = df_new.filter(df_new.total_temperature > 30)

    # Step 6: Sort the result by City in ascending order
    df_final = df_filtered.orderBy("City", ascending=True)
    print(df_final.show())

if __name__ == '__main__':
    city_temperature()