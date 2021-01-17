package dataengine.pipeline.runtime.plan;

import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerException;

public interface PipelineRunner {

    void run() throws DatasetConsumerException;

}
