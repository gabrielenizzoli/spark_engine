package sparkengine.plan.runtime;

import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerException;

public interface PipelineRunner {

    void run() throws DatasetConsumerException;

}
