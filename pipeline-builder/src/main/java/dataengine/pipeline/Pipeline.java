package dataengine.pipeline;

import dataengine.pipeline.datasetconsumer.DatasetConsumer;
import dataengine.pipeline.datasetconsumer.DatasetConsumerException;
import dataengine.pipeline.datasetconsumer.SinkDatasetConsumerFactory;
import dataengine.pipeline.datasetfactory.DatasetFactory;
import dataengine.pipeline.datasetfactory.DatasetFactoryException;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder
public class Pipeline {

    @Nonnull
    DatasetFactory datasetFactory;
    @Nonnull
    SinkDatasetConsumerFactory datasetConsumerFactory;

    public <T> DatasetConsumer<T> run(String factoryName, String consumerName) throws DatasetFactoryException, DatasetConsumerException {
        var dataset = datasetFactory.buildDataset(factoryName);
        var consumer = datasetConsumerFactory.buildConsumer(consumerName);
        return (DatasetConsumer<T>) consumer.readFrom(dataset);
    }

}
