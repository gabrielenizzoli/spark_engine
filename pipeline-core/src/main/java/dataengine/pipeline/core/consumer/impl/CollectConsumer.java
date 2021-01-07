package dataengine.pipeline.core.consumer.impl;

import dataengine.pipeline.core.consumer.DatasetConsumer;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import java.util.LinkedList;
import java.util.List;

@Value
@Builder
public class CollectConsumer<T> implements DatasetConsumer<T> {

    @Getter
    List<T> rows = new LinkedList<>();
    @lombok.Builder.Default
    int limit = 100;

    @Override
    public void accept(Dataset<T> dataset) {
        rows.addAll(dataset.takeAsList(limit));
    }

}
