package sparkengine.plan.model.sink.mapper;

import lombok.AllArgsConstructor;
import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.component.mapper.ComponentMapper;
import sparkengine.plan.model.plan.mapper.DefaultPlanMapper;
import sparkengine.plan.model.plan.mapper.PipelineMapper;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.impl.ForeachSink;
import sparkengine.plan.model.sink.mapper.SinkMapper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@AllArgsConstructor
public class SinkMapperForComponents implements SinkMapper {

    public static final String PLAN = "plan";

    @Nonnull
    protected final ComponentMapper componentMapper;

    @Override
    public final Sink mapForeachSink(Location location, ForeachSink sink) throws Exception {

        var plan = sink.getPlan();

        var planMapper = DefaultPlanMapper.builder()
                .componentMapper(componentMapper)
                .sinkMapper(this)
                .location(location.push(PLAN))
                .build();

        var newPlan = planMapper.map(plan);

        return sink.withPlan(newPlan);
    }

}
