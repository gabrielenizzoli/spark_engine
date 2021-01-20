package sparkengine.plan.runtime.datasetconsumer;

import java.util.Map;

public interface DatasetConsumerFactory {

    static DatasetConsumerFactory ofMap(Map<String, DatasetConsumer> map) {
        return DatasetConsumerFactoryFromMap.of(map);
    }

    <T> DatasetConsumer<T> buildConsumer(String consumerName) throws DatasetConsumerFactoryException;

}
