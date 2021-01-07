package dataengine.pipeline.core.supplier.impl;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nonnull;
import java.util.List;

@Value
@Builder
public class InlineObjectSupplier<T> implements DatasetSupplier<T> {

    @Nonnull
    List<T> objects;
    @Nonnull
    Encoder<T> encoder;

    @Override
    public Dataset<T> get() {
        return SparkSession.active().createDataset(objects, encoder);
    }

}
