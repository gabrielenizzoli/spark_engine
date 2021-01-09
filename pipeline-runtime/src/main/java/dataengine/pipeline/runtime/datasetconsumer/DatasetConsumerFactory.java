package dataengine.pipeline.runtime.datasetconsumer;

public interface DatasetConsumerFactory {

    <T> DatasetConsumer<T> buildConsumer(String consumerName) throws DatasetConsumerFactoryException;

}
