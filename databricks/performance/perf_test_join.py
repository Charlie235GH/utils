#1. Broadcast joins for large-small datasets: broadcast small ref data to all workers, to avoid large data shuffling in joins
#2. Bucketing: to group data with join key hash value, to reduce shuffling during joins, avoid data skews
#3. Repartitioning: to redistribute data evenly (same computational sizes), so to avoid data skewing (small and large partitions, where large partition computations will delay the overall result)
#4. AQE (Adaptive Query Execution): Spark optimizer based on runtime statistics
#Skewed data handling to ensure balanced workflows (dynamically adjust partition sizes: repartition() - split large partitions / coalesce() - merge small partitions)
#Dynamic join strategy selection
#Dynamic partition coalescing (merging of small partitions to reduce join shuffling)
                                                  
# setup spark on linux
from pyspark.sql import *

spark = (SparkSession.builder
         #.master("local") # local- no parallelism at all, local[2] - 2 cores, local[*] - as many cores as local logical cores
         .appName("Perf_test_join-1-standard vs broadcast vs bucketed join")
         .enableHiveSupport() # enableHiveSupport() needed to make data persistent... 
         .config("spark.sql.autoBroadcastJoinThreshold", -1) # max. byte size of table to be broadcast. -1 = disable
         .config("spark.sql.adaptive.enabled", False) # disable AQE
         .getOrCreate())

print('spark version:', spark.version)
print('session Done.')

# generate test data
import random

transaction_list = [
    (f'Product_{random.randint(1, 100)}',  # product_id
     random.randint(1, 1000)               # sales_amount
     )
    for c in range(1000000)]
transaction_df = spark.createDataFrame(transaction_list, ['product_id', 'sales_amount'])
transaction_df.show(5, truncate = False)

ref_list = [
    (f'Product_{c}',                     # product_id
     f'Category_{random.randint(1, 10)}' # category_id
     )
    for c in range(1, 101)]
ref_df = spark.createDataFrame(ref_list, ['product_id', 'category_id'])
ref_df.show(5, truncate = False)

print("data generation Done.")


# 1. standard vs broadcast join
from datetime import datetime
from pyspark.sql.functions import col
from pyspark.sql.functions import broadcast

if 'standard_join_df' in globals():
    del standard_join_df

ct = datetime.now()
standard_join_df = transaction_df.alias('tr').join(ref_df.alias('ref'), col('tr.product_id') == col('ref.product_id'), 'inner')
standard_join_df.count()  # action
print(f"standard_join_df done in {(datetime.now() - ct)} secs.")

ct = datetime.now()
broadcast_join_df = transaction_df.alias('tr').join(broadcast(ref_df).alias('ref'), col('tr.product_id') == col('ref.product_id'), 'inner')
broadcast_join_df.count()  # action
print(f"broadcast_join_df done in {(datetime.now() - ct)} secs.")

broadcast_join_df.show(5, truncate= False)
print("standard_join_df Done.")

# 2. bucketed join (needs to save the data after bucketing.  good for multiple reads)

# 0. delete the tables if exist
spark.sql('DROP TABLE IF EXISTS bucketed_transaction')
spark.sql('DROP TABLE IF EXISTS bucketed_ref')

# 1. save the data in buckets
buckets = 10
transaction_df.write.format('parquet').bucketBy(buckets, 'product_id').saveAsTable('bucketed_transaction')
ref_df.write.format('parquet').bucketBy(buckets, 'product_id').saveAsTable('bucketed_ref')

# 2. read back the data
bucketed_transaction_df = spark.table('bucketed_transaction')
bucketed_ref_df = spark.table('bucketed_ref')

# 3. execute bucketed join
ct = datetime.now()
bucketed_join_df = bucketed_transaction_df.alias('tr').join(bucketed_ref_df.alias('ref'), col('tr.product_id') == col('ref.product_id'), 'inner')
bucketed_join_df.count()  # action
print(f"bucketed_join done in {(datetime.now() - ct)} secs.")
bucketed_join_df.show(5, truncate= False)

print("bucketing join Done.")

# 3. AQE with new config setup (AQE, skewJoins, coalescePartitions enabled, broadcast disabled)
# setup spark on linux
from pyspark.sql import *

try:
    spark.stop()
except:
    None

spark = (SparkSession.builder
         #.master("local") # local- no parallelism at all, local[2] - 2 cores, local[*] - as many cores as local logical cores
		 .appName("Perf_test_join-skewed data with-without-AQE")
         .enableHiveSupport() # enableHiveSupport() needed to make data persistent... 
         .config("spark.sql.adaptive.enabled", True) # enable AQE
         .config("spark.sql.adaptive.skewJoin.enabled", True) # enable skewJoin
         .config("spark.sql.adaptive.coalescePartitions.enabled", True) # enable coalescePartitions
         .config("spark.sql.autoBroadcastJoinThreshold", -1) # max. byte size of table to be broadcast. -1 = disable
         .getOrCreate())

print('spark version:', spark.version)
print('AQE Done.')

# change the transaction test data to be skewed (Product_1 with much higher occurence)
import random

transaction_list = [
    (f'Product_{random.randint(1, 100)  if c < 1000 else 1}',  # product_id
     random.randint(1, 1000)                                   # sales_amount
     )
    for c in range(1000000)]
transaction_df = spark.createDataFrame(transaction_list, ['product_id', 'sales_amount'])
transaction_df.groupBy('product_id').count().orderBy('count', ascending = False).show(5, truncate = False)

ref_list = [
    (f'Product_{c}',                     # product_id
     f'Category_{random.randint(1, 10)}' # category_id
     )
    for c in range(1, 101)]
ref_df = spark.createDataFrame(ref_list, ['product_id', 'category_id'])
ref_df.show(5, truncate = False)

print('generate skewed data Done.')

# join with / without AQE
from datetime import datetime
from pyspark.sql.functions import col

# change config, DISABLE AQE
spark.conf.set("spark.sql.adaptive.enabled", False)  # disable AQE
ct = datetime.now()
non_aqe_join_df = transaction_df.alias('tr').join(ref_df.alias('ref'), col('tr.product_id') == col('ref.product_id'), 'inner')
non_aqe_join_df.count()  # action
print(f"non_AQE_join done in {(datetime.now() - ct)} secs.")

# change config, ENABLE AQE
spark.conf.set("spark.sql.adaptive.enabled", True)  # enable AQE
ct = datetime.now()
aqe_join_df = transaction_df.alias('tr').join(ref_df.alias('ref'), col('tr.product_id') == col('ref.product_id'), 'inner')
aqe_join_df.count()  # action
print(f"AQE_join done in {(datetime.now() - ct)} secs.")
print('join with AQE Done.')

# 3. AQE with new config setup (AQE disabled -> sort merge join, enabled -> broadcasting)
# setup spark on linux
from pyspark.sql import *

try:
    spark.stop()
except:
    None

spark = (SparkSession.builder
         #.master("local") # local- no parallelism at all, local[2] - 2 cores, local[*] - as many cores as local logical cores
		 .appName("Perf_test_join-even data with AQE disabled (sort-merge), enabled (broadcast)")
         .enableHiveSupport() # enableHiveSupport() needed to make data persistent... 
         .config("spark.sql.adaptive.enabled", True) # enable AQE
         .getOrCreate())

print('spark version:', spark.version)
print('join with / without AQE Done.')

# generate test data
import random

transaction_list = [
    (f'Product_{random.randint(1, 100)}',  # product_id
     random.randint(1, 1000)               # sales_amount
     )
    for c in range(1000000)]
transaction_df = spark.createDataFrame(transaction_list, ['product_id', 'sales_amount'])
transaction_df.groupBy('product_id').count().orderBy('count', ascending = False).show(5, truncate = False)

ref_list = [
    (f'Product_{c}',                     # product_id
     f'Category_{random.randint(1, 10)}' # category_id
     )
    for c in range(1, 101)]
ref_df = spark.createDataFrame(ref_list, ['product_id', 'category_id'])
ref_df.show(5, truncate = False)

print('generate even test data Done.')

# join with / without AQE
from datetime import datetime
from pyspark.sql.functions import col

# change config, disable AQE (will not be using broadcast, but sort-merge join strategy)
spark.conf.set("spark.sql.adaptive.enabled", False)  # disable AQE
ct = datetime.now()
non_aqe_join_df = transaction_df.alias('tr').join(ref_df.alias('ref'), col('tr.product_id') == col('ref.product_id'), 'inner')
non_aqe_join_df.count()  # action
print(f"non_AQE_join done in {(datetime.now() - ct)} secs.")

# change config, enable AQE (will be using broadcast)
spark.conf.set("spark.sql.adaptive.enabled", True)  # enable AQE
ct = datetime.now()
aqe_join_df = transaction_df.alias('tr').join(ref_df.alias('ref'), col('tr.product_id') == col('ref.product_id'), 'inner')
aqe_join_df.count()  # action
print(f"AQE_join done in {(datetime.now() - ct)} secs.")
print('join with / without AQE Done.')

# 4. Dynamic partition coalescing
# setup spark on linux
from pyspark.sql import *

try:
    spark.stop()
except:
    None

spark = (SparkSession.builder
         #.master("local") # local- no parallelism at all, local[2] - 2 cores, local[*] - as many cores as local logical cores
		 .appName("Perf_test_join-Dynamic partition coalescing")
         .enableHiveSupport() # enableHiveSupport() needed to make data persistent... 
         .config("spark.sql.adaptive.enabled", True) # enable AQE
         .config("spark.sql.adaptive.coalescePartitions.enabled", True) # will coalesce partitions to be of size advisoryPartitionSizeInBytes
         .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") # size of the partitions to shuffle
         .config("spark.sql.shuffle.Partitions", "20")
         .getOrCreate())

print('spark version:', spark.version)# change the transaction test data to be skewed (Product_1 with much higher occurence)
print('Dynamic partition coalescing Done.')


# generate test data
import random

transaction_list = [
    (f'Product_{random.randint(1, 100)}',  # product_id
     random.randint(1, 1000)               # sales_amount
     )
    for c in range(1000000)]
transaction_df = spark.createDataFrame(transaction_list, ['product_id', 'sales_amount'])
print('generate even test data Done.')

# aggregate

from datetime import datetime
from pyspark.sql.functions import col

# change config, disable coalescePartitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)  # disable coalescePartitions
ct = datetime.now()
none_coalesced_result_df = transaction_df.groupBy('product_id').sum('sales_amount')
none_coalesced_result_df.count() # action
print(f"non_coalesced_aggregate done in {(datetime.now() - ct)} secs.")

# change config, enable AQE (using broadcast)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)  # enable coalescePartitions, spark will dynamically merge (coalesce small partitions)
ct = datetime.now()
coalesced_result_df = transaction_df.groupBy('product_id').sum('sales_amount')
coalesced_result_df.count() # action
print(f"coalesced_aggregate done in {(datetime.now() - ct)} secs.")
print('aggregate Done.')

spark.stop()
print('stop Done.')