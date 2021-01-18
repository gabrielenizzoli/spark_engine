package sparkengine.plan.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.sink.Sink;

import java.util.List;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Plan.Builder.class)
public class Plan {

    Map<String, Component> components;
    Map<String, Sink> sinks;
    List<Pipeline> pipelines;

}
