data = [('2020-05-30','Headphone'),('2020-06-01','Pencil'),('2020-06-02','Mask'),('2020-05-30','Basketball'),('2020-06-01','Book'),('2020-06-02','Mask'),('2020-05-30','T-Shirt')]
columns = ["sell_date", "product"]
df = spark.createDataFrame(data, schema=columns)
df.show()
print("===========Collect list and count====")
filteredData = (df.groupby("sell_date")
                    .agg(
                        collect_list("product").alias("products"),
                        count("product").alias("null_sell")
                    ))

filteredData.show()
