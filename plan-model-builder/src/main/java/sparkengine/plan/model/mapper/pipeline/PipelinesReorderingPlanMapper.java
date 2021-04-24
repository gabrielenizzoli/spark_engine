package sparkengine.plan.model.mapper.pipeline;

import sparkengine.plan.model.plan.mapper.DefaultPlanMapper;
import sparkengine.plan.model.plan.mapper.PlanMapper;
import sparkengine.plan.model.sink.mapper.SinkMapperForPipelines;

public class PipelinesReorderingPlanMapper {

    public static PlanMapper of() {
        var pipelineMapper = new ReorderPipelines();
        var sinkMapper = new SinkMapperForPipelines(pipelineMapper);
        return DefaultPlanMapper.builder()
                .sinkMapper(sinkMapper)
                .pipelineMapper(pipelineMapper)
                .build();
    }

}
