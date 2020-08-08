package dataengine.spark.sql.udf;

import org.apache.spark.sql.api.java.*;
import org.apache.spark.sql.types.DataType;

import javax.annotation.Nonnull;

public interface Udf {

    @Nonnull
    String getName();

    @Nonnull
    DataType getDataType();

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

}
