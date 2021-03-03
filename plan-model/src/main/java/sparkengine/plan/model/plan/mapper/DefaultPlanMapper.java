package sparkengine.plan.model.plan.mapper;

import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.plan.Plan;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.mapper.ComponentMapper;
import sparkengine.plan.model.component.mapper.ComponentsMapper;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.mapper.SinkMapper;
import sparkengine.plan.model.sink.mapper.SinksMapper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@Value
@Builder
public class DefaultPlanMapper implements PlanMapper {

    public static final String COMPONENTS = "components";
    public static final String SINKS = "sinks";

    @Nullable
    ComponentMapper componentMapper;
    @Nullable
    SinkMapper sinkMapper;
    @Nonnull
    @lombok.Builder.Default
    Location location = Location.empty();

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
            return ComponentsMapper.mapComponents(location.push(COMPONENTS), componentMapper, components);
        } catch (Exception | ComponentsMapper.InternalMapperError e) {
            throw new PlanMapperException("exception resolving plan with resolver " + this.getClass().getName(), e);
        }
    }

    private Map<String, Sink> mapAllSinks(@Nonnull Map<String, Sink> sinks)
            throws PlanMapperException {

        if (sinkMapper == null)
            return sinks;

        try {
            return SinksMapper.mapSinks(location.push(SINKS), sinkMapper, sinks);
        } catch (Exception | SinksMapper.InternalMapperError e) {
            throw new PlanMapperException("exception resolving plan with resolver " + this.getClass().getName(), e);
        }
    }

}
