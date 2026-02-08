# performance of count() with different parititioning numbers

from pyspark.sql import *
from datetime import datetime, timedelta
import random
from time import sleep

spark = (SparkSession.builder
         #.master("local")
         .appName("MyPerfApplication")
         .enableHiveSupport() # enableHiveSupport() needed to make data persistent...
         #.config("spark.driver.allowMultipleContexts", "true")
         #.config("spark.sql.cbo.enabled", "true")
         #.config("spark.sql.cbo.optimizer", "true")
         .config("spark.log.level", "ERROR")
         .getOrCreate())

print('spark version:', spark.version)

# prepare test data
l = [
    (c, 
      f'Store_{random.randint(1, 100)}',
      f'Product_{random.randint(1, 1000)}',  
      random.randint(5, 100)
     )
    for c in range(100000)]
rdd = spark.sparkContext.parallelize(l, 20)

print(f"Number of partitions by default: {rdd.getNumPartitions()}")

for row in rdd.take(10):
    print(f"{row[0]}, {row[1]}, {row[2]}, {row[3]}")

# partitions: 1
ct = datetime.now()
rdd = spark.sparkContext.parallelize(l, 1)
discounted_rdd = rdd.map(lambda x: (x[0], x[1], x[2], 0 if x[3] is None else round(x[3]*0.9, 2)))
store_sales_rdd = discounted_rdd.map(lambda x: (x[1], x[3]))
total_sales_rdd = store_sales_rdd.reduceByKey(lambda x, y: x + y)
print(f"Number of partitions: {rdd.getNumPartitions()}, count: {total_sales_rdd.count()}")
print(f"Done in {datetime.now() - ct}")

# partitions: 2
ct = datetime.now()
rdd = spark.sparkContext.parallelize(l, 2)
discounted_rdd = rdd.map(lambda x: (x[0], x[1], x[2], 0 if x[3] is None else round(x[3]*0.9, 2)))
store_sales_rdd = discounted_rdd.map(lambda x: (x[1], x[3]))
total_sales_rdd = store_sales_rdd.reduceByKey(lambda x, y: x + y)
print(f"Number of partitions: {rdd.getNumPartitions()}, count: {total_sales_rdd.count()}")
print(f"Done in {datetime.now() - ct}")

# partitions: 4
ct = datetime.now()
rdd = spark.sparkContext.parallelize(l, 4)
discounted_rdd = rdd.map(lambda x: (x[0], x[1], x[2], 0 if x[3] is None else round(x[3]*0.9, 2)))
store_sales_rdd = discounted_rdd.map(lambda x: (x[1], x[3]))
total_sales_rdd = store_sales_rdd.reduceByKey(lambda x, y: x + y)
print(f"Number of partitions: {rdd.getNumPartitions()}, count: {total_sales_rdd.count()}")
print(f"Done in {datetime.now() - ct}")

# partitions: 20
ct = datetime.now()
rdd = spark.sparkContext.parallelize(l, 20)
discounted_rdd = rdd.map(lambda x: (x[0], x[1], x[2], 0 if x[3] is None else round(x[3]*0.9, 2)))
store_sales_rdd = discounted_rdd.map(lambda x: (x[1], x[3]))
total_sales_rdd = store_sales_rdd.reduceByKey(lambda x, y: x + y)
print(f"Number of partitions: {rdd.getNumPartitions()}, count: {total_sales_rdd.count()}")
print(f"Done in {datetime.now() - ct}")

# partitions: 50
ct = datetime.now()
rdd = spark.sparkContext.parallelize(l, 50)
discounted_rdd = rdd.map(lambda x: (x[0], x[1], x[2], 0 if x[3] is None else round(x[3]*0.9, 2)))
store_sales_rdd = discounted_rdd.map(lambda x: (x[1], x[3]))
total_sales_rdd = store_sales_rdd.reduceByKey(lambda x, y: x + y)
print(f"Number of partitions: {rdd.getNumPartitions()}, count: {total_sales_rdd.count()}")
print(f"Done in {datetime.now() - ct}")


# stop spark session (localhost:4040 will be reset, unaccessible until a new session starts)
print('Sleeping for 60 secs...')
sleep(60)
#spark.stop()
print('Done.')
