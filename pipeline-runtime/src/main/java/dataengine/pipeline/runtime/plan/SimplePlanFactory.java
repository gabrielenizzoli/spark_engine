package dataengine.pipeline.runtime.plan;

import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactory;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactoryException;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactory;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactoryException;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Value
@Builder
public class SimplePlanFactory implements PlanFactory {

    @Nonnull
    List<PipelineName> pipelineNames;
    @Nonnull
    DatasetFactory datasetFactory;
    @Nonnull
    DatasetConsumerFactory datasetConsumerFactory;

    @Override
    public PipelineRunner buildPipelineRunner(PipelineName pipelineName) throws DatasetFactoryException, DatasetConsumerFactoryException {
        var dataset = datasetFactory.buildDataset(pipelineName.getSource());
        var consumer = datasetConsumerFactory.buildConsumer(pipelineName.getDestination());
        return SimplePipelineRunner.builder().dataset(dataset).datasetConsumer(consumer).build();
    }

    @Override
    public List<PipelineRunner> getAllRunners() throws DatasetFactoryException, DatasetConsumerFactoryException {
        List<PipelineRunner> runners = new ArrayList<>(pipelineNames.size());
        for (var pipelineName :pipelineNames)
            runners.add(buildPipelineRunner(pipelineName));
        return Collections.unmodifiableList(runners);
    }
}
