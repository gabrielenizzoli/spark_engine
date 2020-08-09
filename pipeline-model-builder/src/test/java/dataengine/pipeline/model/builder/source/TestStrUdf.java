package dataengine.pipeline.model.builder.source;

import dataengine.spark.sql.udf.Udf;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import javax.annotation.Nonnull;

public class TestStrUdf implements Udf {

    @Nonnull
    @Override
    public String getName() {
        return "testStrUdf";
    }

    @Nonnull
    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }

    @Override
    public UDF2<String, String, String> getUdf2() {
        return (s, p) -> s == null ? s : s + "-" + p;
    }
}
