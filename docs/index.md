# Welcome

## The Problem

Did you ever find yourself writing many (too many) datasets operations (map, flatmap, filter, join, etc etc) and then
making a big data pipeline with them?

Some of your operations are sql statements, others are dataset operations. Some are aggregations, some are joins or
unions. But all are complex and if the plan is very big, they tend to be difficult to manage, debug, and trace.

You program may be something like:

```scala
val df1 = spark.sql("select stuff from source1")
df1.createOrReplaceTempView("table1")

val df2 = sparl.sql("select even more stuff from source 2")
df2.createOrReplaceTempView("table2")

val df3 = spark.sql("select things from table1 join table2 on some id")
df3.createOrReplaceTempView("table3")

val df4 = df3.join(df1).on(some_condition)

\\ and so on ...
df10.write.save
```

You find out that organizing your code like this makes it:

* ... hard to manage
* ... difficult to compose
* ... complex to debug
* ... impossible to maintain and test in isolation
* ... time-consuming to understand where a table is defined
* ... and, do you even remember what the schema of a table is?

## The Solution

You should be going on vacation!

