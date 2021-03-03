package sparkengine.plan.model.plan.mapper;

import lombok.AllArgsConstructor;
import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.component.mapper.ComponentMapper;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.impl.ForeachSink;
import sparkengine.plan.model.sink.mapper.SinkMapper;

import javax.annotation.Nonnull;

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
