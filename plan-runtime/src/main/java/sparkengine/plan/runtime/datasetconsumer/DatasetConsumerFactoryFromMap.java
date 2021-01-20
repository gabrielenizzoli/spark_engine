package sparkengine.plan.runtime.datasetconsumer;

import lombok.Value;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;

@Value(staticConstructor = "of")
public class DatasetConsumerFactoryFromMap implements DatasetConsumerFactory {

    @Nonnull
    Map<String, DatasetConsumer> datasetConsumers;

    @Override
    public <T> DatasetConsumer<T> buildConsumer(String consumerName) throws DatasetConsumerFactoryException {
        return Optional.ofNullable(datasetConsumers.get(consumerName))
                .map(datasetConsumer -> (DatasetConsumer<T>)datasetConsumer)
                .orElseThrow(() -> new DatasetConsumerFactoryException("dataset consumer not found with name " + consumerName));
    }

}
