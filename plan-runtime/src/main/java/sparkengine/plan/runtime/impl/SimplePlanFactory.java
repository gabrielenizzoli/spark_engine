package sparkengine.plan.runtime.impl;

import sparkengine.plan.runtime.PipelineName;
import sparkengine.plan.runtime.PipelineRunner;
import sparkengine.plan.runtime.PlanFactory;
import sparkengine.plan.runtime.PlanFactoryException;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerFactory;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerFactoryException;
import sparkengine.plan.runtime.datasetfactory.DatasetFactory;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;
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
            var dataset = datasetFactory.buildDataset(pipelineName.getDataset());
            var consumer = datasetConsumerFactory.buildConsumer(pipelineName.getConsumer());
            return SimplePipelineRunner.builder().dataset(dataset).datasetConsumer(consumer).build();
        } catch (DatasetConsumerFactoryException | DatasetFactoryException e) {
            throw new PlanFactoryException("can't create pipeline runner for " +pipelineName, e);
        }

    }

}
