package dataengine.pipeline.runtime;

import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactoryException;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactoryException;

public interface PipelineFactory {

    <T> Pipeline<T> buildPipeline(String factoryName, String consumerName)
            throws DatasetFactoryException, DatasetConsumerFactoryException;

}
