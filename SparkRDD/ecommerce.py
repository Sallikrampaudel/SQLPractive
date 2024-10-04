from pyspark import SparkContext

def transformation_rdd():
    data = [
        "1,101,5001,Laptop,Electronics,1000.0,1",
        "2,102,5002,Headphones,Electronics,50.0,2",
        "3,101,5003,Book,Books,20.0,3",
        "4,103,5004,Laptop,Electronics,1000.0,1",
        "5,102,5005,Chair,Furniture,150.0,1"
    ]
    product_category_data = [
        ('Laptop', 'Electronics'),
        ('Headphones', 'Electronics'),
        ('Book', 'Books'),
        ('Chair', 'Furniture')
    ]

    # Setup Spark Context
    sc = SparkContext("local", "E-Commerce Analysis")

    # Create RDDs
    transactions_rdd = sc.parallelize(data)
    transactions_tuple_rdd = transactions_rdd.map(lambda line: line.split(","))

    # Parallelizing product_category_data
    product_category_rdd = sc.parallelize(product_category_data)

    # Transformations
    high_quantity_rdd = transactions_tuple_rdd.filter(lambda x: int(x[6]) > 1)
    products_flat_rdd = transactions_tuple_rdd.flatMap(lambda x: [x[3]])

    # Customer Spending (customer_id, total_spent)
    pair_rdd = transactions_tuple_rdd.map(lambda x: (x[1], (x[3], float(x[5]) * int(x[6]))))
    customer_spending_rdd = pair_rdd.map(lambda x: (x[0], x[1][1])).reduceByKey(lambda x, y: x + y)

    # Products Per Customer (customer_id, list_of_products)
    customer_products_rdd = pair_rdd.map(lambda x: (x[0], x[1][0])).groupByKey().mapValues(list)

    # Join Product Category Information
    product_rdd = pair_rdd.map(lambda x: (x[1][0], (x[0], x[1][1])))
    customer_product_category_rdd = product_rdd.join(product_category_rdd)

    # Actions
    total_spending = customer_spending_rdd.collect()
    products_per_customer = customer_products_rdd.collect()
    product_category_info = customer_product_category_rdd.collect()

    # Output to files (commented out if not needed)
    # customer_spending_rdd.saveAsTextFile("file:///home/takeo/pycharmprojects/customer_spending")
    # customer_products_rdd.saveAsTextFile("file:///home/takeo/pycharmprojects/customer_products")
    customer_product_category_rdd.saveAsTextFile("file:///home/takeo/pycharmprojects/customer_product_category")

    # Stop SparkContext
    sc.stop()

if __name__ == '__main__':
    transformation_rdd()
