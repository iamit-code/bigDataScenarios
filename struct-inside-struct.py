# 🔴 struct inside struct

data="""

{
    "id": 1,
    "trainer": "sai",
    "zeyoAddress": {
        "user": {
            "permanentAddress": "hyderabad",
            "temporaryAddress": "chennai"
        }
    }
}
"""

rdd = sc.parallelize([data])

df = spark.read.option("multiline","true").json(rdd)


df.show()

df.printSchema()


flatdata = df.select(

                    "id",
                    "trainer",
                    "zeyoAddress.user.permanentAddress",
                    "zeyoAddress.user.temporaryAddress",

)

flatdata.show()

flatdata.printSchema()
