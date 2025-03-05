### Task 05 Mar
data = [
    ("1", "a", "10000"),
    ("2", "b", "5000"),
    ("3", "c", "15000"),
    ("4", "d", "25000"),
    ("5", "e", "50000"),
    ("6", "f", "7000")
]
myschema = ["empid","name","salary"]
df = spark.createDataFrame(data, schema=myschema)
df.show()
print()
print("=======Use case to filter by salary")
filTable = df.withColumn("Designation", expr("""
                case 
                    when salary >10000 then 'Manager'
                    else 'Employee'
                end    
                """))
filTable.show()
