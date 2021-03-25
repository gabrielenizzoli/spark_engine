package sparkengine.spark.utils;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import sparkengine.spark.sql.udf.context.UdfContext;
import sparkengine.spark.sql.udf.UdfDefinition;
import sparkengine.spark.sql.udf.context.UdfWithContext;

import javax.annotation.Nonnull;
import java.util.Objects;

public class UdfWithInjectedContext implements UdfDefinition, UdfWithContext, UDF0<String> {

    private Broadcast<UdfContext> udfContext;

    @Override
    public void setUdfContext(@Nonnull Broadcast<UdfContext> udfContext) {
        this.udfContext = Objects.requireNonNull(udfContext);
    }

    @Nonnull
    public String getName() {
        return "writeContext";
    }

    @Nonnull
    public DataType getReturnType() {
        return DataTypes.StringType;
    }

    @Override
    public String call() throws Exception {
        return udfContext.getValue().toString();
    }

}
