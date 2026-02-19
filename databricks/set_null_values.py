from pyspark.sql import functions as F

# 1. Define your list of columns
cols_to_fix = df.columns 

# 2. Build the transformation expressions
# This replaces both actual Nulls and the string "NULL" with "UNDEFINED"
transformations = [
    F.when(F.col(c).isNull() | (F.col(c) == "NULL"), "UNDEFINED")
     .otherwise(F.col(c))
     .alias(c) 
    for c in cols_to_fix
]

# 3. Apply all transformations in a single projection
df_cleaned = df.select(*transformations)

# Show a sample to verify
df_cleaned.display()
