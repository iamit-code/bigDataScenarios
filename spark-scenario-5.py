### SCENARIO 5
from pyspark.sql.window import Window

data1 = [
    (1, "A", "A", 1000000),
    (2, "B", "A", 2500000),
    (3, "C", "G", 500000),
    (4, "D", "G", 800000),
    (5, "E", "W", 9000000),
    (6, "F", "W", 2000000),
]
df1 = spark.createDataFrame(data1, ["emp_id","name","dept_id","salary"])
df1.show()
data2 = [("A", "AZURE"), ("G", "GCP"), ("W", "AWS")]
df2 = spark.createDataFrame(data2, ["dept_id1","dept_name"])
df2.show()
deptwindow = Window.partitionBy("dept_id").orderBy(col("salary").desc())
finaldata = (df1.withColumn("drank", dense_rank().over(deptwindow))
                .filter("drank = 2").join(df2, df1["dept_id"]==df2["dept_id1"], "inner")
                .drop("dept_id").drop("drank").drop("dept_id1")
        )
print("===========dense Rank=======")
finaldata.show()
