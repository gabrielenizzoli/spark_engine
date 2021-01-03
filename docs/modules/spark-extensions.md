# The spark-extension module

This module has the main utility functionalities that are needed in the rest of the project.
This module only depends on spark, with no additional libraries involved. 
It provides some basic extension and manipulation facilities for handling UDFs, logical plans, and datasets.

| Package | Language | Description |
| ----------- | ----------- | ----------- |
| `dataengine.spark.sql.logicalplan` | `java` | Java utility (`SqlCompiler`) that injects in the logical plan uresolved references to tables and udfs. |
| `dataengine.scala.compat` | `scala` | Scala functions wrappers for Java functions. Useful to provide java functions in scala-only apis. |
| `dataengine.spark.transformation` | `java` | Utility classes that encapsulates dataset transformation logic. |

## SqlCompiler

In spark sql, to be able to reference a table or a udf, a developer must first register the table or udf in a catalog. Example with spark sql:
```java
var ds = ...;
ds.createOrReplaceTempView("table");
var dsNew = spark.sql("select column from table");
```

Contrary to that, the programmatic interface does not need this but simply requires a reference to the dataset. Example with programmatic interface:
```java
var ds = ...;
var newDs = ds.select("column");
```

While the programmatic interface is useful, many times **a full execution plan is better expressed as a set of separated sql statements**.
When the statements are just too many, then it would be nice to be able to chain them without having to resort to a table catalog.
These statements then can be tested in isolation, and finally connected at a later stage.
A class called `SqlCompiler` allows chaining sql statements together as if they were a programmatic interface, bypassing the spark catalog. Example:

```java
import dataengine.spark.sql.logicalplan.SqlCompiler;
import dataengine.spark.sql.logicalplan.tableresolver.Table;

var ds = ...;
        
var sqlCompiler = SqlCompiler.builder()
        .tableResolver(Table.ofDataset("table", ds))
        .build();

var newDs = sqlCompiler.compileSqlToDataset(sparkSession, "select column from table")
```

The same is possible with UDFs (and UDAFs). Example:

```java
import dataengine.spark.sql.logicalplan.SqlCompiler;
import dataengine.spark.sql.logicalplan.tableresolver.Table;
import dataengine.spark.sql.udf.Udf;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class UdfPlusOne implements Udf {
    @Nonnull
    public String getName() {
        return "plusOne";
    }

    @Nonnull
    public DataType getReturnType() {
        return DataTypes.IntegerType;
    }

    public UDF1<Integer, Integer> getUdf1() {
        return i -> i + 1;
    }
}

var ds = ...;
        
var sqlCompiler = SqlCompiler.builder()
        .tableResolver(Table.ofDataset("table", ds))
        .functionResolver(new UdfPlusOne())
        .build();

var newDs = sqlCompiler.sql("select plusOne(column) from table")
```

Note that a given `SqlCompiler` is only able to resolve the relations (aka, Tables) and udfs that are defined at the creation of the compiler itself.
Any other relation or udf that can't be resolved will result in an exception.
This will allow for a fully controlled sql statement, where all the unknown elements (relations and udf) MUST be specified ahead of time.
No more surprises, with developers in a large organization defining (in unknown places) tables or udf.


## Utilities

The main goals of facilities in this package is to have function wrappers between java and scala.

### Function/BiFunction wrappers

Two classes are provided to wrap generic java functions, depending on the number of the input parameters.

| Class | Extends | Wraps |
| ----------- | ----------- | ----------- |
| `JavaToScalaFunction1` | `scala.Function1` | `java.util.function.Function` |
| `JavaToScalaFunction2` | `scala.Function2` | `java.util.function.BiFunction` |

Example:
```java
import java.util.function.Function;
import dataengine.scala.compat.JavaToScalaFunction1;

Function<Integer, Integer> javaFunction = i -> i+1;
JavaToScalaFunction1<Integer, Integer> scalaFunction = new JavaToScalaFunction1<>(javaFunction);
```

### Udf Wrappers

Similar utilities with the same purpose as the ones above, but this time to wrap UDF java functions.

| Class | Extends | Wraps |
| ----------- | ----------- | ----------- |
| `JavaUdf0ToScalaFunction0` | `scala.Function0` | `org.apache.spark.sql.api.java.UDF0` |
| `JavaUdf1ToScalaFunction1` | `scala.Function1` | `org.apache.spark.sql.api.java.UDF1` |
| `JavaUdf1ToScalaFunction2` | `scala.Function2` | `org.apache.spark.sql.api.java.UDF2` |
| `JavaUdf1ToScalaFunction3` | `scala.Function3` | `org.apache.spark.sql.api.java.UDF3` |
| `JavaUdf1ToScalaFunction4` | `scala.Function4` | `org.apache.spark.sql.api.java.UDF4` |
| `JavaUdf1ToScalaFunction5` | `scala.Function5` | `org.apache.spark.sql.api.java.UDF5` |

Example:
```java
import org.apache.spark.sql.api.java.UDF1;

UDF1<Integer, Integer> javaUdf = i -> i+1;
JavaUdf1ToScalaFunction1<Integer, Integer> scalaUdf = new JavaUdf1ToScalaFunction1<>(javaUdf);
```

### Dataset Transformations 

These classes abstract and encapsulates some combination logic between datasets. 
They are useful to divide and organize a complex data flow between many source and destination points. 
Example of a simple transformation:
```java
import dataengine.spark.transformation.DataTransformation2;

DataTransformation2<Integer, Integer, Integer> tx = (d1, d2) -> d1.as("d1").join(d2.as("d2"), col("d1.value").equalTo(col("d2.value")));

var ds1 = ...;
var ds2 = ...;
var dsResult = tx.apply(ds1, ds2);
```

Each transformation interface has a method called `apply()` that takes a variable number of datasets and provides, as an output, a dataset.
Every transformation class moreover has 2 additional useful methods, called `andThen` and `andThenEncode`.
They allow for easy chaining additional changes to a new transformation object. 
This makes for an easier sequential reading of the code, and requires the developer to call `apply()` only once.
Example:
```java
import dataengine.spark.transformation.DataTransformation;
import dataengine.spark.transformation.DataTransformation2;


Dataset<Integer> ds1 = ...;
Dataset<Long> ds2 = ...;
DataTransformation2<Integer, Integer, Row> transformation1 = ...;
DataTransformation<Row, Row> transformation2 = ...;

// without chaining
Dataset<SampleClass> dsNewWithoutChaining = transformation2
        .apply(transformation1.apply(ds1, ds2))
        .as(Encoders.bean(SampleClass.class));

// with chaining
Dataset<SampleClass> dsNewWithChaining = transformation1
        .andThen(transformation2)
        .andThenEncode(Encoders.bean(SampleClass.class))
        .apply(ds1, ds2);

```

List of transformation classes available:

| Interface | Inputs | Outputs | Notes |
| ----------- | ----------- | ----------- | ----------- |
| `DataTransformation<S, D>` | `Dataset<S>` | `Dataset<D>` | 1 input dataset |
| `DataTransformation2<S1, S2, D>` | `Dataset<S1>`, `Dataset<S2>` | `Dataset<D>` | 2 input datasets of same or different types |
| `DataTransformation3<S1, S2, S3, D>` | `Dataset<S1>`, `Dataset<S2>`, `Dataset<S3>` | `Dataset<D>` | 3 input datasets of same or different types |
| ... | ... | ... | ... |
| `DataTransformation10<S1, ..., S10, D>` | `Dataset<S1>`, ..., `Dataset<S10>` (10 datasets) | `Dataset<D>` | 10 input datasets of same or different types |
| `DataTransformationN<S, D>` | `List<Dataset<S>>` | `Dataset<D>` | N input datasets of same type |

### Sql Transformations 

The `Transformations` class has some `sql` method to quickly wrap a sql statement and a `SqlCompiler` together to create a transformation. 

Example on how to create **transformations based on sql statements**:
```java
import dataengine.spark.transformation.Transformations;

// 1 dataset
var ds1 = ...;
var sql = "select column from table";
var newDs = Transformations.sql("table", sql).apply(ds1);

// 2 datasets
var sql2 = "select table1.column from table1 join table2 on table1.column = table2.column";
var newDs2 = Transformations.sql("table1", "table2", sql2).apply(ds1, ds2);

// if input datasets ate Dataset<Row>
var newDs3 = Transformations.sql(List.of("table1", "table2"), sql2).apply(List.of(ds1, ds2));
```

Sql transformation methods in `dataengine.spark.transformation.Transformations` are:

| Method | Parameters | Output | Note |
| ----------- | ----------- | ----------- | ----------- |
| `sql()` | `sourceName`, `sql` | `DataTransformation<S, Row>` | Transform an input dataset `Dataset<S>` to a `Dataset<Row>` using a sql statement defined in the `sql` variable, where the input dataset is referenced by the `sourceName` name. |
| `sql()` | `sourceName`, `sql`, `sqlFunctions` | `DataTransformation<S, Row>` | Transform an input dataset `Dataset<S>` to a `Dataset<Row>` using a sql statement defined in the `sql` variable, where the input dataset is referenced by the `sourceName` name. Note that the list of functions in `sqlFunctions` will be used to resolve udfs in the sql statement. |
| `sql()` | `sourceNames`, `sql` | `DataTransformationN<Row, Row>` | Transform a list of input datasets of type `Dataset<Row>` to a `Dataset<Row>` using a sql statement defined in the `sql` variable, where the input datasets are referenced by the value of the list `sourceNames`. |
| `sql()` | `sourceNames`, `sql`, `sqlFunctions` | `DataTransformationN<Row, Row>` | Transform a list of input datasets of type `Dataset<Row>` to a `Dataset<Row>` using a sql statement defined in the `sql` variable, where the input datasets are referenced by the value of the list `sourceNames`. Note that the list of functions in `sqlFunctions` will be used to resolve udfs in the sql statement. |

If you wish to enumerate the input datasets (and they all have different type) you can use these methods :

| Method | Parameters | Output | Note |
| ----------- | ----------- | ----------- | ----------- |
| `sql()` | `sourceName1`, `sourceName2`, `sql` | `DataTransformation<S1, S2, Row>` | Transform input datasets of type `Dataset<S1>` and `Dataset<S2>` to a `Dataset<Row>` using a sql statement defined in the `sql` variable, where the input datasets are referenced by the `sourceName1` and `sourceName2` names respectively. |
| `sql()` | `sourceName1`, `sourceName2`, `sourceName3`, `sql` | `DataTransformation<S1, S2, S3, Row>` | Transform input datasets of type `Dataset<S1>`, `Dataset<S2>`, and `Dataset<S3>` to a `Dataset<Row>` using a sql statement defined in the `sql` variable, where the input datasets are referenced by the `sourceName1`, `sourceName2` and `sourceName3` names respectively. |
| ... | ... | ... | ... |

### Other Transformations

The `Transformations` class holds additional method for quickly creating new dataset transformations.

Easily implement a **dataset transformation around a simple java function**. Example:
```java
Dataset<Integer> ds = ...;

// map function
var newDs = Transformations.map((Integer i) -> i+1, Encoders.INT());

// flat map function
var newDsWithFlatMap = Transformations.flatMap((Integer i) -> IntStream.range(i, i+10).iterator(), Encoders.INT());
```

Chain transformations to **cache and/or encode datasets**. Example:
```java
import dataengine.spark.transformation.Transformations;

Dataset<Row> ds = ...;

Dataset<Integer> newDs = Transformations
        .encodeAs(Encoders.INT)
        .cache(StorageLevel.DISK_ONLY)
        .apply(ds);
```

