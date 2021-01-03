package dataengine.spark.utils;

import dataengine.spark.sql.udf.Udf;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import javax.annotation.Nonnull;

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