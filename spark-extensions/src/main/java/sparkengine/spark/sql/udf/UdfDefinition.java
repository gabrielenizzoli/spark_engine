package sparkengine.spark.sql.udf;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.api.java.*;
import org.apache.spark.sql.types.DataType;
import sparkengine.spark.sql.logicalplan.functionresolver.UnresolvedFunctionReplacer;
import sparkengine.spark.sql.logicalplan.functionresolver.UnresolvedUdfReplacer;
import sparkengine.spark.sql.udf.context.UdfContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An interface that provides a Java Udf.
 * Many methods are available to return the udf (each with a different number of input parameters).
 * The first method that return a non-empty Udf is the one used.
 */
public interface UdfDefinition extends SqlFunction {

    @Nonnull
    DataType getReturnType();

    default <R> UDF0<R> asUdf0() {
        if (this instanceof UDF0)
            return (UDF0<R>) this;
        return null;
    }

    default <I1, R> UDF1<I1, R> asUdf1() {
        if (this instanceof UDF1)
            return (UDF1<I1, R>) this;
        return null;
    }

    default <I1, I2, R> UDF2<I1, I2, R> asUdf2() {
        if (this instanceof UDF2)
            return (UDF2<I1, I2, R>) this;
        return null;
    }

    default <I1, I2, I3, R> UDF3<I1, I2, I3, R> asUdf3() {
        if (this instanceof UDF3)
            return (UDF3<I1, I2, I3, R>) this;
        return null;
    }

    default <I1, I2, I3, I4, R> UDF4<I1, I2, I3, I4, R> asUdf4() {
        if (this instanceof UDF4)
            return (UDF4<I1, I2, I3, I4, R>) this;
        return null;
    }

    default <I1, I2, I3, I4, I5, R> UDF5<I1, I2, I3, I4, I5, R> asUdf5() {
        if (this instanceof UDF5)
            return (UDF5<I1, I2, I3, I4, I5, R>) this;
        return null;
    }

    static <I1, R> UdfDefinition wrapUdf1(String name, DataType returnType, UDF1<I1, R> udf1) {

        return new UdfDefinition() {
            @Nonnull
            @Override
            public String getName() {
                return name;
            }

            @Nonnull
            @Override
            public DataType getReturnType() {
                return returnType;
            }

            @Override
            public UDF1<I1, R> asUdf1() {
                return udf1;
            }

        };

    }

    @Nonnull
    @Override
    default UnresolvedFunctionReplacer asFunctionReplacer(@Nullable Broadcast<UdfContext> udfContextBroadcast) {
        return new UnresolvedUdfReplacer(this, udfContextBroadcast);
    }

}
