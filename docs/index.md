# Compose spark dataset operations with ease

Did you ever find yourself writing many many datasets operations and then making a single data pipeline with them?

Something like:
```scala
val df1 = spark.sql("select stuff from source1")
df1.createOrReplaceTempView("table1")
val df2 = sparl.sql("select even more stuff from source 2")
df2.createOrReplaceTempView("table2")
val df3 = spark.sql("select things from table1 join table2 on some id")
df3.createOrReplaceTempView("table3")

\\ and so on ...

df10.write.save
```

Well, probably you find out that they are:
* difficult to write
* difficult to compose
* difficult to debug
* difficult to maintain
* difficult to understand where a table is defined 
* ... and do you remember what the schema of a table is?
