from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

def wordcount(filepath):
    rdd = spark.sparkContext.textFile(filepath)
    rdd2 = rdd.flatMap(lambda x: x.split(" "))
    rdd3 = rdd2.map(lambda x: (x, 1))
    rdd5 = rdd3.reduceByKey(lambda a, b: a + b)
    return rdd5

def structType():
    data = [("James", "", "Smith", "36636", "M", 3000),
            ("Michael", "Rose", "", "40288", "M", 4000),
            ("Robert", "", "Williams", "42114", "M", 4000),
            ("Maria", "Anne", "Jones", "39192", "F", 4000),
            ("Jen", "Mary", "Brown", "", "F", -1)
            ]

    schema = StructType([ \
        StructField("firstname", StringType(), True), \
        StructField("middlename", StringType(), True), \
        StructField("lastname", StringType(), True), \
        StructField("id", StringType(), True), \
        StructField("gender", StringType(), True), \
        StructField("salary", IntegerType(), True) \
        ])

    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.show()


def nestedStructType():
    structureData = [
        (("James", "", "Smith"), "36636", "M", 3100),
        (("Michael", "Rose", ""), "40288", "M", 4300),
        (("Robert", "", "Williams"), "42114", "M", 1400),
        (("Maria", "Anne", "Jones"), "39192", "F", 5500),
        (("Jen", "Mary", "Brown"), "", "F", -1)
    ]
    structureSchema = StructType([
        StructField('name', StructType([
            StructField('firstname', StringType(), True),
            StructField('middlename', StringType(), True),
            StructField('lastname', StringType(), True)
        ])),
        StructField('id', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('salary', IntegerType(), True)
    ])

    df2 = spark.createDataFrame(data=structureData, schema=structureSchema)
    df2.printSchema()
    df2.show(truncate=False)


if __name__ == '__main__':
    spark: SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").getOrCreate()
    #rdd5=wordcount("file:///home/takeo/test.txt")
    #print(rdd5.collect())

    structType()
    nestedStructType()