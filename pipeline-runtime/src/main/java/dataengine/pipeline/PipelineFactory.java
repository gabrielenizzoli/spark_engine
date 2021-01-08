package dataengine.pipeline;

import dataengine.pipeline.datasetconsumer.DatasetConsumerFactory;
import dataengine.pipeline.datasetconsumer.DatasetConsumerFactoryException;
import dataengine.pipeline.datasetfactory.DatasetFactory;
import dataengine.pipeline.datasetfactory.DatasetFactoryException;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder
public class PipelineFactory {

    @Nonnull
    DatasetFactory datasetFactory;
    @Nonnull
    DatasetConsumerFactory datasetConsumerFactory;

    public <T> Pipeline<T> buildPipeline(String factoryName, String consumerName)
            throws DatasetFactoryException, DatasetConsumerFactoryException {
        var dataset = datasetFactory.buildDataset(factoryName);
        var consumer = datasetConsumerFactory.buildConsumer(consumerName);
        var pipeline = Pipeline.builder().dataset(dataset).datasetConsumer(consumer).build();
        return (Pipeline<T>) pipeline;
    }


}
