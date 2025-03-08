### Aggregation
# FULL AGGREGATION CODE
from pyspark.sql.functions import *
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
df = spark.createDataFrame(data, ["name", "city", "amount"]).coalesce(1)
# Show the DataFrame
df.show()
print("======== SUM PER EACH NAME=========")
aggdf1 = df.groupBy( "name" ).agg(  sum("amount").alias("total") )
aggdf1.show()
print("======== SUM AND COUNT PER EACH NAME=========")
aggdf2 = df.groupBy("name").agg(
    sum("amount").alias("total")  ,
    count("amount").alias("cnt")
)
aggdf2.show()
print("========== SUM  per each name and city=======")
aggdf3 = df.groupBy( "name" , "city" ).agg(
                                    sum("amount").alias("total") , 
                                    count("amount").alias("cnt")
                        )
aggdf3.show()
