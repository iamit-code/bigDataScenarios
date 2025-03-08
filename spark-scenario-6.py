### duplicate value Drop
data = [(1, "John", "Testing", 5000),
        (2, "Tim", "Development", 6000),
        (3, "Jhon", "Development", 5000),
        (4, "Sky", "Prodcution", 8000)]
df = spark.createDataFrame(data, ["id", "name", "dept", "salary"])
df.show()
noDuplicateDepartment = df.dropDuplicates(["dept"]).orderBy("id")
print()
print("======Delete Duplicate=====")
noDuplicateDepartment.show()
