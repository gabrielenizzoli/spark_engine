package sparkengine.plan.model.mapper;

import lombok.AllArgsConstructor;
import lombok.Value;
import sparkengine.plan.model.LocationUtils;
import sparkengine.plan.model.component.mapper.ComponentMapper;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.impl.ForeachSink;
import sparkengine.plan.model.sink.mapper.SinkMapper;

import javax.annotation.Nonnull;
import java.util.Stack;

@AllArgsConstructor
public class SinkMapperForComponents implements SinkMapper {

    public static final String PLAN = "plan";

    @Nonnull
    protected final ComponentMapper componentMapper;

    @Override
    public Sink mapForeachSink(Stack<String> location, ForeachSink sink) throws Exception {

        var plan = sink.getPlan();

        var planMapper = DefaultPlanMapper.builder()
                .componentMapper(componentMapper)
                .sinkMapper(this)
                .location(LocationUtils.push(location, PLAN))
                .build();

        var newPlan = planMapper.map(plan);

        return sink.withPlan(newPlan);
    }

}
