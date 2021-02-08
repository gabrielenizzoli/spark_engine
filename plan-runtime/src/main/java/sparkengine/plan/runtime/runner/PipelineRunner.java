package sparkengine.plan.runtime.runner;

import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerException;

public interface PipelineRunner {

    void run() throws DatasetConsumerException;

}
