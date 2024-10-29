"""
Problem Statement
●	Covid-19 data analysis

Use Cases
Section 1
●	Rename the column infection_case to infection_source
●	Select only following columns
○	 'Province','city',infection_source,'confirmed'
●	Change the datatype of confirmed column to integer
●	Return the TotalConfirmed and MaxFromOneConfirmedCase for each "province","city" pair
●	Sort the output in asc order on the basis of confirmed
●
Section 2
●	Return the top 2 provinces on the basis of confirmed cases.
Section 3
●	Return the details only for ‘Daegu’ as province name where confirmed cases are more than 10
●	Select the columns other than latitude, longitude and case_id
○
drop_list = ['latitude', 'longitude', 'case_id',]
df = df.select([column for column in df.columns if column not in drop_list])
○
●
Submission
●	Pyspark code

"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def covid_analysis():
    spark = SparkSession.builder.appName("Covid19-Data-Analysis-Use-Cases").getOrCreate()
    CovidCases_df = spark.read.option("header", True).csv("file:///home/takeo/test/covid.csv")

    #To rename the column infection_case to infection_source
    new_renamed_df = CovidCases_df.withColumnRenamed("infection_case",  "infection_source")

    #Select only following columns 'Province','city',infection_source,'confirmed'
    selected_column_df = new_renamed_df.select('province','city','infection_source','confirmed')

    #Change the datatype of confirmed column to integer
    selected_column_df=selected_column_df.withColumn("confirmed",selected_column_df["confirmed"].cast("integer"))

    #Return the TotalConfirmed and MaxFromOneConfirmedCase for each "province","city" pair Sort the output in asc order on the basis of confirmed
    result_df = selected_column_df.groupBy("province","city").agg(
        F.sum("confirmed").alias("TotalConfirmed"),
        F.max("confirmed").alias("MaxFromOneConfirmedCase")
    )

    #Sort the output in asc order on the basis of confirmed
    result_df = result_df.sort('TotalConfirmed',ascending=True)

    #Return the top 2 provinces on the basis of confirmed cases.
    result_df = CovidCases_df.sort("TotalConfirmed", ascending=False).limit(2)

    print(result_df.show())
    print(result_df.printSchema())

if __name__ == '__main__':
    covid_analysis()