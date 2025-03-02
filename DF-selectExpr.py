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


#### Expression #############
exprdf = df.selectExpr(
    "cast(id as int) as id",
        "split(tdate, '-')[2] as year",
        "amount+100 as amount",
        "upper(category) as category",
        "concat(product, '~amit') as product",
        "spendby",
        "case when spendby='cash' then 0 else 1 end as status"
)
print()
print("===========EXpr with Split method==========")
print()
exprdf.show()

exprDataFuncdf = df.selectExpr(
    "cast(id as int) as id",
    "YEAR(to_date(tdate, 'MM-dd-yyyy')) as year",
    "amount+100 as amount",
    "upper(category) as category",
    "concat(product, '~amit') as product",
    "spendby",
    "case when spendby='cash' then 0 else 1 end as status"
)
print()
print("===========EXpr with Date Function==========")
print()
exprDataFuncdf.show()
