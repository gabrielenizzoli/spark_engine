package dataengine.pipeline.runtime.datasetconsumer;

import org.apache.spark.sql.Dataset;

public interface DatasetConsumer<T> {

    DatasetConsumer<T> readFrom(Dataset<T> dataset) throws DatasetConsumerException;

}
