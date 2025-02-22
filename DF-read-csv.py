#### *** Read DATA and convert to DF ****

csvdf = spark.read.format("csv").option("header", "true").load("usdata.csv")
csvdf.createOrReplaceTempView("zeyo")
procdf = spark.sql("select * from zeyo where state='LA'")
print()
print("===============CSV DF=======")
print()
csvdf.show()

# Process using SQL to filter only records have state='LA'
csvdf.createOrReplaceTempView("zeyo")
procdf = spark.sql("select * from zeyo where state='LA'")
print()
print("===============SQL PROCESS DF=======")
print()
procdf.show()