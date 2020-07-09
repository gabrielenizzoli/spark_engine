package dataengine.pipeline.core.source.impl;

import dataengine.pipeline.core.source.DataSource;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;

import javax.annotation.Nonnull;

@Value
@Builder
public class EmptyDatasetSource<T> implements DataSource<T> {

    @Nonnull
    Encoder<T> encoder;

    @Override
    public Dataset<T> get() {
        return SparkSession.active().emptyDataset(encoder);
    }

}
