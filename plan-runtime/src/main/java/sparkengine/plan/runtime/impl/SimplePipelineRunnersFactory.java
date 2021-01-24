package sparkengine.plan.runtime.impl;

import lombok.Builder;
import lombok.Value;
import sparkengine.plan.runtime.PipelineName;
import sparkengine.plan.runtime.PipelineRunner;
import sparkengine.plan.runtime.PipelineRunnersFactory;
import sparkengine.plan.runtime.PipelineRunnersFactoryException;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerFactory;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerFactoryException;
import sparkengine.plan.runtime.datasetfactory.DatasetFactory;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;

import javax.annotation.Nonnull;
import java.util.List;

@Value
@Builder
public class SimplePipelineRunnersFactory implements PipelineRunnersFactory {

    @Nonnull
    List<PipelineName> pipelineNames;
    @Nonnull
    DatasetFactory datasetFactory;
    @Nonnull
    DatasetConsumerFactory datasetConsumerFactory;

    @Override
    public PipelineRunner buildPipelineRunner(PipelineName pipelineName) throws PipelineRunnersFactoryException {

        if (!pipelineNames.contains(pipelineName))
            throw new PipelineRunnersFactoryException.PipelineNotFound(pipelineName.toString());

        try {
            var dataset = datasetFactory.buildDataset(pipelineName.getDataset());
            var consumer = datasetConsumerFactory.buildConsumer(pipelineName.getConsumer());
            return SimplePipelineRunner.builder().dataset(dataset).datasetConsumer(consumer).build();
        } catch (DatasetConsumerFactoryException | DatasetFactoryException e) {
            throw new PipelineRunnersFactoryException("can't create pipeline runner for " + pipelineName, e);
        }

    }

}
