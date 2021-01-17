package dataengine.pipeline.runtime.plan;

import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactoryException;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactoryException;

import java.util.List;

public interface PlanFactory {

    List<PipelineName> getPipelineNames();

    PipelineRunner buildPipelineRunner(PipelineName key) throws DatasetFactoryException, DatasetConsumerFactoryException;

    List<PipelineRunner> getAllRunners() throws DatasetFactoryException, DatasetConsumerFactoryException;

}
