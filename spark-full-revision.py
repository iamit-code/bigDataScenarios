#FULL REVISION CODE

filerdd = sc.textFile("file1.txt")

gymrdd = filerdd.filter(lambda x : 'Gymnastics' in x)

print("====== FILTER GYMNASTICS=====")
print()

mapsplit = gymrdd.map( lambda x  :  x.split(",") )

from collections import namedtuple

columns = namedtuple('columns', ['txnno','txndate','custno','amount','category','product','city','state','spendby'])

schemardd = mapsplit.map(lambda x : columns(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8]))

prodfilted = schemardd.filter(lambda x : 'Gymnastics' in x.product)

prodfilted.foreach(print)


schemadf = prodfilted.toDF()

print()
print("=====SCHEMA DF======")
print()
schemadf.show(5)



csvdf = spark.read.format("csv").option("header","true").load("file3.txt")
print()
print("=====csvdf DF======")
print()
csvdf.show(5)



jsondf = spark.read.format("json").load("file4.json").select('txnno','txndate','custno','amount','category','product','city','state','spendby')
print()
print("=====jsondf DF======")
print()
jsondf.show(5)



parquetdf = spark.read.load("file5.parquet")
print()
print("=====parquetdf DF======")
print()
parquetdf.show(5)


uniondf = schemadf.union(csvdf).union(jsondf).union(parquetdf)
print()
print("=====uniondf DF======")
print()
uniondf.show(5)



procdf =(
        uniondf.withColumn("txndate" , expr("split(txndate,'-')[2]"))
                .withColumnRenamed("txndate", "year")
                .withColumn("status",expr("case when spendby='cash' then 0 else 1 end"))
                .filter("txnno>50000")

)

print()
print("=====procdf DF======")
print()
procdf.show(5)



aggdf = procdf.groupBy("category").agg(sum("amount").alias("total"))

print()
print("=====aggdf DF======")
print()
aggdf.show()


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



inner = cust.join(prod, ["id"], "inner")
print()
print("======INNER=====")
print()
inner.show()


left = cust.join(prod, ["id"], "left")
print()
print("======left=====")
print()
left.show()




right = cust.join(prod, ["id"], "right")
print()
print("======right=====")
print()
right.show()


full = cust.join(prod, ["id"], "full")
print()
print("======full=====")
print()
full.show()




anti = cust.join(prod, ["id"], "left_anti")
print()
print("======anti=====")
print()
anti.show()



cross = cust.crossJoin(prod)
print()
print("======cross=====")
print()
cross.show()



from pyspark.sql.functions import *
data = [
        ("DEPT1", 1000),
        ("DEPT1", 700),
        ("DEPT1", 500),
        ("DEPT2", 400),
        ("DEPT2", 200),
        ("DEPT3", 500),
        ("DEPT3", 200)]

columns = ["dept", "salary"]

df = spark.createDataFrame(data, columns)

df.show()


##🔴🔴🔴🔴🔴🔴 STEP 1 --- CREATE THE WINDOW

from pyspark.sql.window import Window
deptwindow = Window.partitionBy("dept").orderBy( col("salary").desc() )

##🔴🔴🔴🔴🔴🔴 STEP 2 --- APPLYING WITH WINDOW  ON DATAFRAME TO DENSE RANK

drank = df.withColumn("drank" , dense_rank().over(deptwindow)       )
drank.show()

##🔴🔴🔴🔴🔴🔴 STEP 3  -- FILTER RANK =2
filrank = drank.filter(" drank = 2 ")
filrank.show()
