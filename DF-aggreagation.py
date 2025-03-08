### Aggregation
from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.master("local").appName("DataFrame Example").getOrCreate()

# Define the data as a list of tuples
data = [
    ('sai', 'chn', 1),
    ('sai', 'hyd', 2),
    ('sai', 'chn', 2),
    ('sai', 'hyd', 1),
    ('zeyo', 'chn', 2),
    ('zeyo', 'hyd', 3),
    ('zeyo', 'chn', 2),
    ('zeyo', 'hyd', 1)
]

# Create a DataFrame using the data and specifying the column names
df = spark.createDataFrame(data, ["name","city","amount"]).coalesce(1)
df.show()
print("====SUM per each name======")
aggdf1= df.groupby("name").agg( sum("amount").alias("total"))
aggdf1.show()
print("========SUM AND COUNT==========")
aggdf2 = df.groupby("name").agg(
                    sum("amount").alias("total"),
                    count("amount").alias("cnt")
                )
print()
aggdf2.show()
