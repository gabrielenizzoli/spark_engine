package sparkengine.plan.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.sink.Sink;

import java.util.List;
import java.util.Map;

@Value
@Builder(setterPrefix = "with", toBuilder = true)
@JsonDeserialize(builder = Plan.Builder.class)
public class Plan {

    @With
    Map<String, Component> components;
    Map<String, Sink> sinks;
    Map<String, Pipeline> pipelines;

}
