package dataengine.pipeline.runtime.datasetconsumer;

import org.apache.spark.sql.Dataset;

public interface DatasetConsumer<T> {

    void readFrom(Dataset<T> dataset) throws DatasetConsumerException;

}
