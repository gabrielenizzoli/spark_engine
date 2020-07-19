package dataengine.pipeline.core.source.impl;

import dataengine.pipeline.core.source.DataSource;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

@Value
@Builder
public class InlineJsonDataframeSource implements DataSource<Row> {

    @Nonnull
    List<String> json;
    @Nonnull
    String schema;

    @Override
    public Dataset<Row> get() {
        DataFrameReader reader = SparkSession.active().read();
        if (schema != null) {
            StructType structType = StructType.fromDDL(schema);
            reader = reader.schema(structType);
        }
        Dataset<String> ds = SparkSession.active().createDataset(json, Encoders.STRING());
        return reader.json(ds);
    }

}
