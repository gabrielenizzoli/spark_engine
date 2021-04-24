package sparkengine.plan.model.plan;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.sink.Sink;

import java.util.Map;

@Value
@Builder(setterPrefix = "with", toBuilder = true)
@JsonDeserialize(builder = Plan.PlanBuilder.class)
public class Plan {

    @With
    Map<String, Component> components;
    Map<String, Sink> sinks;
    @With
    Map<String, Pipeline> pipelines;

}
