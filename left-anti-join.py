###  left_anti join
Create df1
df1 = spark.createDataFrame([("1",), ("2",), ("3",)], ["col"])
# Show df1
df1.show()
# Create df2
df2 = spark.createDataFrame([("1",), ("2",), ("3",), ("4",), ("5",)], ["col"])
# Show df2
df2.show()
maxdf = df1.selectExpr(" max(col) as col")
maxdf.show()

joindf = df2.join(maxdf, ["col"], "left_anti")
print()
print("===left anti join=====")
joindf.show()
