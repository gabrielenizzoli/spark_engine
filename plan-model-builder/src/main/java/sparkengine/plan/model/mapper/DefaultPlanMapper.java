package sparkengine.plan.model.mapper;

import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import sparkengine.plan.model.Plan;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.mapper.ComponentMapper;
import sparkengine.plan.model.component.mapper.ComponentsMapper;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.mapper.SinkMapper;
import sparkengine.plan.model.sink.mapper.SinksMapper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@Value(staticConstructor = "of")
public class DefaultPlanMapper implements PlanMapper {

    @Nullable
    ComponentMapper componentMapper;
    @Nullable
    SinkMapper sinkMapper;

    @Override
    public @Nonnull
    Plan map(@Nonnull Plan plan) throws PlanMapperException {
        var components = mapAllComponents(plan.getComponents());
        var sinks = mapAllSinks(plan.getSinks());
        return plan.toBuilder()
                .withComponents(components)
                .withSinks(sinks)
                .build();
    }

    private Map<String, Component> mapAllComponents(@Nonnull Map<String, Component> components)
            throws PlanMapperException {

        if (componentMapper == null)
            return components;

        try {
            return ComponentsMapper.mapComponents(ComponentsMapper.locationEmpty(), componentMapper, components);
        } catch (Exception | ComponentsMapper.InternalMapperError e) {
            throw new PlanMapperException("exception resolving pan with resolver " + this.getClass().getName(), e);
        }
    }

    private Map<String, Sink> mapAllSinks(@Nonnull Map<String, Sink> sinks)
            throws PlanMapperException {

        if (sinkMapper == null)
            return sinks;

        try {
            return SinksMapper.mapSinks(sinkMapper, sinks);
        } catch (Exception | SinksMapper.InternalMapperError e) {
            throw new PlanMapperException("exception resolving pan with resolver " + this.getClass().getName(), e);
        }
    }

}
