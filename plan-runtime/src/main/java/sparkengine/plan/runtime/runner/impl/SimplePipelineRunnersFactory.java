package sparkengine.plan.runtime.runner.impl;

import lombok.Builder;
import lombok.Value;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerFactory;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerFactoryException;
import sparkengine.plan.runtime.datasetfactory.DatasetFactory;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;
import sparkengine.plan.runtime.runner.PipelineRunner;
import sparkengine.plan.runtime.runner.PipelineRunnersFactory;
import sparkengine.plan.runtime.runner.PipelineRunnersFactoryException;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Value
@Builder
public class SimplePipelineRunnersFactory implements PipelineRunnersFactory {

    @Value(staticConstructor = "of")
    public static class PipelineDefinition {

        @Nonnull
        String name;
        @Nonnull
        String dataset;
        @Nonnull
        String consumer;

    }

    @Nonnull
    List<PipelineDefinition> pipelineDefinitions;
    @Nonnull
    DatasetFactory datasetFactory;
    @Nonnull
    DatasetConsumerFactory datasetConsumerFactory;

    @Override
    public List<String> getPipelineNames() {
        return pipelineDefinitions.stream().map(PipelineDefinition::getName).collect(Collectors.toList());
    }

    @Override
    public PipelineRunner buildPipelineRunner(String pipelineName) throws PipelineRunnersFactoryException {

        var optionalPipelineDefinition = pipelineDefinitions.stream().filter(pd -> pd.getName().equals(pipelineName)).findFirst();
        if (optionalPipelineDefinition.isEmpty())
            throw new PipelineRunnersFactoryException.PipelineNotFound(pipelineName);

        try {
            var pipelineDef = optionalPipelineDefinition.get();
            var dataset = datasetFactory.buildDataset(pipelineDef.getDataset());
            var consumer = datasetConsumerFactory.buildConsumer(pipelineDef.getConsumer());
            return SimplePipelineRunner.builder().dataset(dataset).datasetConsumer(consumer).build();
        } catch (DatasetConsumerFactoryException | DatasetFactoryException e) {
            throw new PipelineRunnersFactoryException(String.format("can't create pipeline runner for [%s], pipeline definition [%s]", pipelineName, optionalPipelineDefinition), e);
        }

    }


}
