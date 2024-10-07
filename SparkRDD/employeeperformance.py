from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, coalesce
from pyspark.sql.window import Window

def performance():
    data = [
        (1, '2024-01-10', 'Engineering', 5, 'John'),
        (2, '2024-01-11', 'HR', 4, 'Jane'),
        (3, '2024-01-12', 'Sales', 3, 'Sam'),
        (4, '2024-02-01', 'Engineering', 5, 'John'),
        (1, '2024-03-10', 'Engineering', 4, 'Jane'),
        (2, '2024-03-11', 'HR', None, 'Sam')
    ]
    spark = SparkSession.builder.appName("Employee Performance Review Analysis").getOrCreate()

    columns = ["emp_id", "review_date", "department", "rating", "reviewer"]
    df = spark.createDataFrame(data, schema=columns)

    df_filtered = df.filter(df.rating >= 4)
    df_no_duplicates = df.dropDuplicates(['emp_id', 'review_date'])

    df_selected = df.select('emp_id', 'department', 'rating')

    df_grouped = df.groupBy('department').agg({'rating': 'avg'})

    df_employees = df.select('emp_id', 'department')
    df_joined = df.join(df_employees, on='emp_id', how='inner')

    df_union = df.union(df_new_reviews)


    print(df_union.show())


if __name__ == '__main__':
    performance()