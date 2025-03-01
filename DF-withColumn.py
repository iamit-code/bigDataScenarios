### withColumns
data = [
    ("00000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00005", "02-14-2011", 200, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()

### withColumns
procwith = (
    df.withColumn("category", expr("upper(category)"))
        .withColumn("id", expr("cast(id as int)"))
        .withColumn("year", expr("split(tdate, '-')[2]"))
        .withColumn("amount", expr("amount+100"))
        .withColumn("product", expr("concat(product, '~zeyo')"))
        .withColumn("status", expr("case when spendby='cash' then 1 else 0 end"))
).select("status","spendby","category","product","id","year","amount")
print()
print("===========withColumn==========")
print()
procwith.show()
