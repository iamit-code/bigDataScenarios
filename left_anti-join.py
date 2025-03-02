# 🔴 LEFT_ANTI Note there is no right_anti join so need to use left_anti smartly by changing the order
from pyspark.sql.functions import *

data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]

cust = spark.createDataFrame(data4, ["id", "name"]).coalesce(1)
cust.show()
data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]
prod = spark.createDataFrame(data3, ["id", "product"]).coalesce(1)
prod.show()
prodlist = prod.select("id").rdd.flatMap(lambda x : x).collect()
print(prodlist)
print()
print()
fildf = cust.filter(~col("id").isin(prodlist))
fildf.show()

antijoin = cust.join(prod,["id"], "left_anti")
antijoin.show()
