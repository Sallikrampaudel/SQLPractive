from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

def movierating():
    data = [
        (1, 101, 4, 1622388000000),
        (1, 102, 3, 1622388020000),
        (2, 101, 5, 1622388040000),
        (2, 103, 4, 1622388060000),
        (3, 101, 5, 1622388080000),
        (3, 102, 4, 1622388100000),
        (3, 103, None, 1622388120000),
        (4, 101, 2, 1622388140000),
        (4, 101, 2, 1622388140000)
    ]
    spark = SparkSession.builder.appName("Movie Ratings Analysis").getOrCreate()

    columns = ["user_id", "movie_id", "rating", "timestamp"]
    df = spark.createDataFrame(data, schema=columns)
    #print(df.show())

    df_filtered = df.filter(df.rating >= 4)
    #print(df_filtered.show())

    average_rating = df.selectExpr('avg(rating)').collect()[0][0]
    df_filled = df.na.fill({'rating': average_rating})
    #print(df_filled.show())


    df_no_duplicates = df.dropDuplicates(['user_id', 'movie_id'])


    df_selected = df.select('user_id', 'rating')
    #print(df_selected.show())

    df_grouped = df.groupBy('movie_id').agg({'rating': 'avg'})
    #print(df_grouped.show())

    movies = [
        (101, "Venom"),
        (102, "Chaka Panja 5"),
        (103, "Tazza Khabar")
    ]
    movie_columns  = ["movie_id", "movie_name"]
    df_movies= spark.createDataFrame(movies, movie_columns)

    #Join the 'movie_ratings' DataFrame with a 'movie_details' DataFrame that contains 'movie_id' and 'movie_name'.
    # Assuming df_movies is another DataFrame that contains movie details
    df_joined = df.join(df_movies, on='movie_id', how='inner')
    #print(df_joined.show())

    df_union = df.union(df)
    #print(df_union.show())

    df.createOrReplaceTempView('ratings')
    sql_result = spark.sql(
        'SELECT movie_id, AVG(rating) as avg_rating FROM ratings GROUP BY movie_id ORDER BY avg_rating LIMIT 10')
    print(sql_result.show())

if __name__ == '__main__':
    movierating()