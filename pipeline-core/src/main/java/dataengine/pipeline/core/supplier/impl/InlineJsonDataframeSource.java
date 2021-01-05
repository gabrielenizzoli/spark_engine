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
public class InlineJsonDataframeSource implements DatasetSupplier<Row> {

    @Nonnull
    List<String> json;
    @Nonnull
    String schema;

    @Override
    public Dataset<Row> get() {
        DataFrameReader reader = SparkSession.active().read().schema(StructType.fromDDL(schema));
        Dataset<String> ds = SparkSession.active().createDataset(json, Encoders.STRING());
        return reader.json(ds);
    }

}
