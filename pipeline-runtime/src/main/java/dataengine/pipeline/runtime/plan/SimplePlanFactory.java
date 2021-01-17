package dataengine.pipeline.runtime.plan;

import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactory;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactoryException;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactory;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactoryException;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
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
    public PipelineRunner buildPipelineRunner(PipelineName pipelineName) throws PlanFactoryException {

        if (!pipelineNames.contains(pipelineName))
            throw new PlanFactoryException.PipelineNotFound(pipelineName.toString());

        try {
            var dataset = datasetFactory.buildDataset(pipelineName.getSource());
            var consumer = datasetConsumerFactory.buildConsumer(pipelineName.getDestination());
            return SimplePipelineRunner.builder().dataset(dataset).datasetConsumer(consumer).build();
        } catch (DatasetConsumerFactoryException | DatasetFactoryException e) {
            throw new PlanFactoryException("can't create pipeline runner for " +pipelineName, e);
        }

    }

}
