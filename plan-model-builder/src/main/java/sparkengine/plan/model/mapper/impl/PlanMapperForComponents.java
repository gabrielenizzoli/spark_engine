package sparkengine.plan.model.mapper.impl;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import sparkengine.plan.model.Plan;
import sparkengine.plan.model.component.mapper.ComponentMapper;
import sparkengine.plan.model.mapper.DefaultPlanMapper;
import sparkengine.plan.model.mapper.PlanMapper;
import sparkengine.plan.model.mapper.PlanMapperException;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.impl.ForeachSink;
import sparkengine.plan.model.sink.mapper.SinkMapper;

import javax.annotation.Nonnull;

public class PlanMapperForComponents implements PlanMapper {

    ComponentMapper componentMapper;

    @Nonnull
    @Override
    public Plan map(@Nonnull Plan plan) throws PlanMapperException {
        var planMapper = DefaultPlanMapper.of(componentMapper, new SinkMapperForComponents());
        return planMapper.map(plan);
    }

    @Value
    public class SinkMapperForComponents implements SinkMapper {

        @Override
        public Sink mapForeachSink(ForeachSink sink) throws Exception {
            var plan = sink.getPlan();
            var planMapper = DefaultPlanMapper.of(componentMapper, this);
            var newPlan = planMapper.map(plan);
            return sink.withPlan(newPlan);
        }

    }

}
