# Shuffling ("exchange" in query plan): produce datasets that can be transfered onto separate data nodes, and manipulated as sub-tasks (e.g. sub-joins) to avoid transfering all the data multiple times across joins, etc.
# one dataset would have all the ROWS of one or more value of the join key, making sure that join will have full sub-result of that value

# used in operations such as groupby(), join(), repartition(), distinct()

# smaller datasets (not compared to each other but in spark settings "spark.sql.autoBroadcastJoinThreshold") are broadcasted so that shuffling would not happen!

from pyspark.sql import *

spark = (SparkSession.builder
         #.master("local")
         .appName("Shuffle perf test with unordered vs ordered join keys")
         .enableHiveSupport() # enableHiveSupport() needed to make data persistent...
         .config("spark.log.level", "ERROR")
         .config("spark.executor.memory", "1g")
         .config("spark.driver.memory", "1g")
         .getOrCreate())

print('spark version:', spark.version)

# generate some test data
from pyspark.sql import functions as F
import random
import math

num_airports =   50000 # max. 26^4
num_flights  = 10000000
num_delays   = 10000000
num_alerts   = 10000000

# origin_airport
letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
num_airports = min(num_airports, pow(len(letters), 4))
origin_airport_lst = [
    ( 
        
        ''.join([letters[c1 // pow(len(letters), c) % len(letters)] for c in range(4)]),                                # airport                                        # airport
        (
            ''.join([letters[c1 // pow(len(letters), c) % len(letters)] for c in range(4)]) +                              # display_airport_name
            ' ' +
            ''.join([letters[random.randint(0, len(letters)-1)] for c in range(random.randint(5, 10))])
        ).title(),
     )
    for c1 in range(num_airports)]

origin_airport_df = spark.createDataFrame(origin_airport_lst, ['airport', 'display_airport_name']).select(
    F.col('airport').alias('origin'), 
    F.col('display_airport_name').alias('origin_display_name'))
origin_airport_df.show(5, truncate=False)

# flights
flights_lst = [
    (c1,                                                               # flight_id
     origin_airport_lst[c1%(len(origin_airport_lst))][0],              # origin
     origin_airport_lst[(c1 + 1)%(len(origin_airport_lst))][0]         # dest
     )
    for c1 in range(num_flights)]
flights_df = spark.createDataFrame(flights_lst, ['flight_id', 'origin', 'dest'])
flights_df.show(5, truncate=False)

def non_negative(n):
    return 0 if n < 0 else n

# delays
delays_lst = [
    (c1,                                            # flight_id
     non_negative(random.randint(-40, 35)),        # dep_delay_mins
     )
    for c1 in range(num_delays)]
delays_df = spark.createDataFrame(delays_lst, ['flight_id', 'dep_delay_mins'])
delays_df.show(5, truncate=False)

# flight_alerts
flight_alerts_lst = [
    (c1,                                                   # flight_id
     "some alert!" if random.randint(1,10) <= 3 else ""    # alert
     )
    for c1 in range(num_alerts)]
flight_alerts_df = spark.createDataFrame(flight_alerts_lst, ['flight_id', 'alert'])
flight_alerts_df.show(5, truncate=False)
print("data generation Done.")


# join test
from datetime import datetime

# join keys VARY in execution order => reshuffling is necessary!
ct = datetime.now()
result_df = (
    flights_df
    .select("flight_id", "origin", "dest")
    .join(delays_df, "flight_id", "left") # flight_id shuffle
    .join(origin_airport_df, "origin", "left") # origin shuffle
    .join(flight_alerts_df, "flight_id", "left") # flight_id re-shuffled
)

#result_df = result_df.na.fill(value="", subset = ['alert'])
result_df.write.mode("overwrite").csv("result_df1") # to make sure all results are used, not only a few that's shown
print(f"Unsorted join key done in {(datetime.now() - ct)} secs.")

# join keys DO NOT VARY in execution order => re-shuffling is NOT necessary!
ct = datetime.now()
result_df = (
    flights_df
    .select("flight_id", "origin", "dest")
    .join(origin_airport_df, "origin", "left") # origin shuffle
    .join(delays_df, "flight_id", "left") # flight_id shuffle
    .join(flight_alerts_df, "flight_id", "left") # flight_id, no re-shuffling needed
)

#result_df = result_df.na.fill(value="", subset = ['alert'])
result_df.write.mode("overwrite").csv("result_df2") # to make sure all results are used, not only a few that's shown
print(f"Sorted join key done in {(datetime.now() - ct)} secs.")
print('join test Done.')

spark.stop()
print('Done.')