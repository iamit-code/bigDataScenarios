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

finaldf  = filrank.drop("drank")
finaldf.show()
