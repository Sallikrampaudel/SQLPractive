jdbc:redshift://default-workgroup.654654294059.us-east-2.redshift-serverless.amazonaws.com:5439/dev

df_query = spark.read.format("jdbc").option("url", "jdbc:redshift://default-workgroup.654654294059.us-east-2.redshift-serverless.amazonaws.com:5439/dev").option("query", "select * from dev.test.listing limit 2").option("driver","com.amazon.redshift.jdbc42.Driver").option("user", "admin").option("password", "Admin1234").load()

df1 = spark.read.format("jdbc").option("url", "jdbc:redshift://default-workgroup.654654294059.us-east-2.redshift-serverless.amazonaws.com:5439/dev").option("query", "select * from dev.test.listing limit 2").option("driver","com.amazon.redshift.jdbc42.Driver").option("user", "admin").option("password", "Admin1234").load()

df.write.format("jdbc").option("url", "jdbc:redshift://default-workgroup.654654294059.us-east-2.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "dev.test.employee").option("driver","com.amazon.redshift.jdbc42.Driver").option("user", "admin").option("password", "Admin1234").mode("overwrite").save()