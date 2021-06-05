package sparkengine.plan.model.plan.mapper;

import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.mapper.ComponentMapper;
import sparkengine.plan.model.component.mapper.ComponentsMapper;
import sparkengine.plan.model.plan.Pipeline;
import sparkengine.plan.model.plan.Plan;
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
    @Nullable
    PipelineMapper pipelineMapper;
    @Nonnull
    @lombok.Builder.Default
    Location location = Location.empty();

    @Override
    public @Nonnull
    Plan map(@Nonnull Plan plan) throws PlanMapperException {
        return plan.toBuilder()
                .withComponents(mapAllComponents(plan.getComponents()))
                .withSinks(mapAllSinks(plan.getSinks()))
                .withPipelines(mapPipelines(plan.getPipelines()))
                .build();
    }

    private Map<String, Component> mapAllComponents(@Nonnull Map<String, Component> components)
            throws PlanMapperException {

        if (componentMapper == null)
            return components;

        try {
            return ComponentsMapper.mapComponents(location.push(COMPONENTS), componentMapper, components);
        } catch (Exception | ComponentsMapper.InternalMapperError e) {
            throw new PlanMapperException(String.format("exception mapping components with plan mapper %s, component mapper %s",
                    this.getClass().getName(), componentMapper.getClass().getName()), e);
        }
    }

    private Map<String, Sink> mapAllSinks(@Nonnull Map<String, Sink> sinks)
            throws PlanMapperException {

        if (sinkMapper == null)
            return sinks;

        try {
            return SinksMapper.mapSinks(location.push(SINKS), sinkMapper, sinks);
        } catch (Exception | SinksMapper.InternalMapperError e) {
            throw new PlanMapperException(String.format("exception mapping sinks with plan mapper %s, sink mapper %s",
                    this.getClass().getName(), sinkMapper.getClass().getName()), e);
        }
    }

    private Map<String, Pipeline> mapPipelines(Map<String, Pipeline> pipelines)
            throws PlanMapperException {

        if (pipelineMapper == null)
            return pipelines;

        try {
            return pipelineMapper.mapPipelines(pipelines);
        } catch (Exception e) {
            throw new PlanMapperException(String.format("exception mapping pipelines with plan mapper %s, pipeline mapper %s",
                    this.getClass().getName(), pipelineMapper.getClass().getName()), e);
        }
    }

}
