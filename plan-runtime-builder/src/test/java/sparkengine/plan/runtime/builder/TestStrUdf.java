package sparkengine.plan.runtime.builder;

import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import sparkengine.spark.sql.udf.UdfDefinition;

import javax.annotation.Nonnull;

public class TestStrUdf implements UdfDefinition {

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
    public UDF2<String, String, String> asUdf2() {
        return (s, p) -> s == null ? s : s + "-" + p;
    }
}
