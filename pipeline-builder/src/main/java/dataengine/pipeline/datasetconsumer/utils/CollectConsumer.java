package dataengine.pipeline.datasetconsumer.utils;

import dataengine.pipeline.datasetconsumer.DatasetConsumer;
import org.apache.spark.sql.Dataset;

import java.util.List;

public class CollectConsumer<T> implements DatasetConsumer<T> {

    public CollectConsumer(int limit) {
        this.limit = limit;
    }

    private int limit;
    private List<T> list = List.of();

    @Override
    public void readFrom(Dataset<T> dataset) {
        list = List.copyOf(dataset.takeAsList(limit));
    }

}
