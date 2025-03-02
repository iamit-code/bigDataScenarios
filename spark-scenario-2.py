###
#Filtering a DF from the parent and child table with new column as grandParent
###
data = [("A", "AA"), ("B", "BB"), ("C", "CC"), ("AA", "AAA"), ("BB", "BBB"), ("CC", "CCC")]
df = spark.createDataFrame(data, ["child", "parent"])
df.show()
df1  = df
df2  = df.withColumnRenamed("child","child1").withColumnRenamed("parent","parent1")
df1.show()
df2.show()
inner = df1.join(df2,  df1["parent"]== df2["child1"]  , "inner")
inner.show()
finaldf1 = inner.drop("child1")
finaldf1.show()
finaldf = finaldf1.withColumnRenamed("parent1", "Grandparent")
finaldf.show()
