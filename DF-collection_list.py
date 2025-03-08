## Collect_list
# collList = df.groupby("name", "city").agg(
#                                 collect_list("city").alias("city-collection"),
#                                 collect_list("amount").alias("collection")
#                             )
# collList.show()
data = [(1, "Mark Ray", "AB"),
        (2, "Peter Smith", "CD"),
        (1, "Mark Ray", "EF"),
        (2, "Peter Smith", "GH"),
        (2, "Peter Smith", "CD"),
        (3, "Kate", "IJ")]

myschema = ["custid", "custname", "address"]

df = spark.createDataFrame(data, schema=myschema)
df.show()
dropDupDf = df.drop_duplicates()
dropDupDf.show()
print()
print("========")
collectList = dropDupDf.groupby("custname").agg(
                            collect_list("address").alias("address_list")
                        )
collectList.show()
