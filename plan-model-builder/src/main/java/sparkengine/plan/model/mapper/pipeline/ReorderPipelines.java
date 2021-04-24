package sparkengine.plan.model.mapper.pipeline;

import sparkengine.plan.model.plan.Pipeline;
import sparkengine.plan.model.plan.PipelineSorter;
import sparkengine.plan.model.plan.mapper.PipelineMapper;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

public class ReorderPipelines implements PipelineMapper {

    @Override
    public Map<String, Pipeline> mapPipelines(@Nonnull Map<String, Pipeline> pipelines) throws Exception {

        int order = 0;
        Map<String, Pipeline> newPipelines = new HashMap<>();
        for (var entry : PipelineSorter.orderPipelines(pipelines)) {
            newPipelines.put(entry.getKey(), entry.getValue().withOrder(order++));
        }

        return newPipelines;
    }

}
