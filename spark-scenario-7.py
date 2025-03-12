# ### SCENARIO 7

data = [(1,"Veg Biryani"),(2,"Veg Fried Rice"),(3,"Kaju Fried Rice"),
        (4,"Chicken Biryani"),(5,"Chicken Dum Biryani"),(6,"Prawns Biryani"),
        (7,"Fish Birayani")]
df1 = spark.createDataFrame(data, ["food_id","food_item"])
df1.show()
ratings = [(1,5),(2,3),(3,4),(4,4),(5,5),(6,4),(7,4)]
df2 = spark.createDataFrame(ratings,["food_id","rating"])
df2.show()

joindata = (df1.join(df2, "food_id", "inner")
                .withColumn("stats(out of 5)",
                            expr("repeat('*', rating) ")))
joindata.show()
