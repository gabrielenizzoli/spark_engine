package sparkengine.plan.runtime.runner.impl;

import lombok.Builder;
import lombok.Value;
import sparkengine.plan.runtime.runner.PipelineRunner;
import sparkengine.plan.runtime.runner.PipelineRunnersFactory;
import sparkengine.plan.runtime.runner.PipelineRunnersFactoryException;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerFactory;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerFactoryException;
import sparkengine.plan.runtime.datasetfactory.DatasetFactory;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;

@Value
@Builder
public class SimplePipelineRunnersFactory implements PipelineRunnersFactory {

    @Value(staticConstructor = "of")
    public static class PipelineDefinition {

        @Nonnull
        String dataset;
        @Nonnull
        String consumer;

    }

    @Nonnull
    Map<String, PipelineDefinition> pipelineDefinitions;
    @Nonnull
    DatasetFactory datasetFactory;
    @Nonnull
    DatasetConsumerFactory datasetConsumerFactory;

    @Override
    public Set<String> getPipelineNames() {
        return pipelineDefinitions.keySet();
    }

    @Override
    public PipelineRunner buildPipelineRunner(String pipelineName) throws PipelineRunnersFactoryException {

        var pipelineDef = pipelineDefinitions.get(pipelineName);
        if (pipelineDef == null)
            throw new PipelineRunnersFactoryException.PipelineNotFound(pipelineName);

        try {
            var dataset = datasetFactory.buildDataset(pipelineDef.getDataset());
            var consumer = datasetConsumerFactory.buildConsumer(pipelineDef.getConsumer());
            return SimplePipelineRunner.builder().dataset(dataset).datasetConsumer(consumer).build();
        } catch (DatasetConsumerFactoryException | DatasetFactoryException e) {
            throw new PipelineRunnersFactoryException(String.format("can't create pipeline runner for [%s], pipeline definition [%s]", pipelineName, pipelineDef), e);
        }

    }


}
