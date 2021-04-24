package sparkengine.plan.model.sink.mapper;

import lombok.AllArgsConstructor;
import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.component.mapper.ComponentMapper;
import sparkengine.plan.model.plan.mapper.DefaultPlanMapper;
import sparkengine.plan.model.plan.mapper.PipelineMapper;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.impl.ForeachSink;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@AllArgsConstructor
public class SinkMapperForPipelines implements SinkMapper {

    public static final String PLAN = "plan";

    @Nonnull
    protected final PipelineMapper pipelineMapper;

    @Override
    public final Sink mapForeachSink(Location location, ForeachSink sink) throws Exception {

        var plan = sink.getPlan();

        var planMapper = DefaultPlanMapper.builder()
                .sinkMapper(this)
                .pipelineMapper(pipelineMapper)
                .location(location.push(PLAN))
                .build();

        var newPlan = planMapper.map(plan);

        return sink.withPlan(newPlan);
    }

}
