package dataengine.pipeline.runtime;

import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactory;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactoryException;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactory;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactoryException;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder
public class SimplePipelineFactory implements PipelineFactory {

    @Nonnull
    DatasetFactory datasetFactory;
    @Nonnull
    DatasetConsumerFactory datasetConsumerFactory;

    @Override
    public <T> Pipeline<T> buildPipeline(String factoryName, String consumerName)
            throws DatasetFactoryException, DatasetConsumerFactoryException {
        var dataset = datasetFactory.buildDataset(factoryName);
        var consumer = datasetConsumerFactory.buildConsumer(consumerName);
        var pipeline = Pipeline.builder().dataset(dataset).datasetConsumer(consumer).build();
        return (Pipeline<T>) pipeline;
    }


}
