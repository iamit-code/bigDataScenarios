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








innerjoin = cust.join(prod , ["id"] , "inner")

print("======INNER JOIN======")
print()
innerjoin.show()



left = cust.join(prod, ["id"], "left")

print("======left JOIN======")
print()
left.show()

right = cust.join(prod, ["id"] , "right")

print("======right JOIN======")
print()
right.show()

full = cust.join(prod, ["id"] , "full" )

print("======full JOIN======")
print()
full.show()

prod1 = spark.createDataFrame(data3, ["id1", "product"]).coalesce(1)
prod1.show()

inner = cust.join(prod1 ,   cust["id"] == prod1["id1"]    , "inner" ).drop("id1")
inner.show()
