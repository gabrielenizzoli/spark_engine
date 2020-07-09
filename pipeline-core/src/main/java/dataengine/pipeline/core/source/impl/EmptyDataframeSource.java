package dataengine.pipeline.core.source.impl;

import dataengine.pipeline.core.source.DataSource;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nonnull;
import java.util.Collections;

@Value
@Builder
public class EmptyDataframeSource implements DataSource<Row> {

    @Nonnull
    String schema;

    @Override
    public Dataset<Row> get() {
        StructType structType = StructType.fromDDL(schema);
        return SparkSession.active().createDataFrame(Collections.emptyList(), structType);
    }

}
