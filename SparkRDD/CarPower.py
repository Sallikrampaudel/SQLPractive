from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

def carpower():
    data= [
        ('Ford Torino', 140, 3449, 'US'),
        ('Chevrolet Monte Carlo', 150, 3761, 'US'),
        ('BMW 2002', 113, 2234, 'Europe')
    ]
    columns = ["carr", "horsepower", "weight","origin"]

    spark = SparkSession.builder.appName("Car Power").getOrCreate()

    df=spark.createDataFrame(data, columns)

    df_car_correction = df.withColumnRenamed("carr", 'car')

    df_transformed =df_car_correction.withColumn("AvgWeight", lit(200)).withColumn("kilowatt_power", col("horsepower") * 1000)
    print(df_transformed.show())



if __name__ == '__main__':
    carpower()