package sparkengine.plan.runtime.datasetconsumer;

public interface DatasetConsumerFactory {

    <T> DatasetConsumer<T> buildConsumer(String consumerName) throws DatasetConsumerFactoryException;

}
