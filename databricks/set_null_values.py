from pyspark.sql import functions as F

# 1. Define your list of columns
cols_to_fix = df.columns

# 2. Build the transformation expressions
# This replaces both actual Nulls and the string "NULL" with "UNDEFINED"
transformations = [
    F.when((F.col(field).isNull() | (F.lower(F.col(field)) == F.lit("null"))), F.lit("undefined"))
    .otherwise(F.col(field))
    .alias(field) 
    for field in field_to_update
]

# 3. Apply all transformations in a single projection
df = df.select(*transformations)

# Show a sample to verify
df_cleaned.display()
