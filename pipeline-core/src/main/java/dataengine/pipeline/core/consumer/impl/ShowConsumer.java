package dataengine.pipeline.core.consumer.impl;

import dataengine.pipeline.core.consumer.DatasetConsumer;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

@Value
@Builder
public class ShowConsumer<T> implements DatasetConsumer<T> {

    @lombok.Builder.Default
    int numRows = 20;
    @lombok.Builder.Default
    int truncate = 30;

    @Override
    public void accept(Dataset<T> dataset) {
        dataset.show(numRows, truncate);
    }

}
