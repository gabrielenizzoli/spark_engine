package dataengine.pipeline.core.supplier.impl;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nonnull;
import java.util.Collections;

@Value
@Builder
public class EmptyDataframeSupplier implements DatasetSupplier<Row> {

    @Nonnull
    String schema;

    @Override
    public Dataset<Row> get() {
        StructType structType = StructType.fromDDL(schema);
        return SparkSession.active().createDataFrame(Collections.emptyList(), structType).toDF();
    }

}
