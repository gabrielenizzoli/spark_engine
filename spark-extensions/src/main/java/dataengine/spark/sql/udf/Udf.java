package dataengine.spark.sql.udf;

import org.apache.spark.sql.api.java.*;
import org.apache.spark.sql.types.DataType;

import javax.annotation.Nonnull;

public interface Udf extends SqlFunction {

    @Nonnull
    DataType getReturnType();

    default <R> UDF0<R> getUdf0() {
        return null;
    }

    default <I1, R> UDF1<I1, R> getUdf1() {
        return null;
    }

    default <I1, I2, R> UDF2<I1, I2, R> getUdf2() {
        return null;
    }

    default <I1, I2, I3, R> UDF3<I1, I2, I3, R> getUdf3() {
        return null;
    }

    default <I1, I2, I3, I4, R> UDF4<I1, I2, I3, I4, R> getUdf4() {
        return null;
    }

    default <I1, I2, I3, I4, I5, R> UDF5<I1, I2, I3, I4, I5, R> getUdf5() {
        return null;
    }

    static <I1, R> Udf ofUdf1(String name, DataType returnType, UDF1<I1, R> udf1) {

        return new Udf() {
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
            public UDF1<I1, R> getUdf1() {
                return udf1;
            }

        };
    }

}
