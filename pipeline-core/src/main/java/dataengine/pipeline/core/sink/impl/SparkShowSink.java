package dataengine.pipeline.core.sink.impl;

import dataengine.pipeline.core.sink.DataSink;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

@Value
@Builder
public class SparkShowSink<T> implements DataSink<T> {

    @lombok.Builder.Default
    int numRows = 20;
    @lombok.Builder.Default
    int truncate = 30;

    @Override
    public void accept(Dataset<T> dataset) {
        dataset.show(numRows, truncate);
    }

}
