package dataengine.pipeline.datasetconsumer;

public interface DatasetConsumerFactory {

    <T> DatasetConsumer<T> buildConsumer(String consumerName) throws DatasetConsumerFactoryException;

}
