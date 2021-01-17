package dataengine.pipeline.model.plan;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.component.Component;
import dataengine.pipeline.model.sink.Sink;
import lombok.Builder;
import lombok.Value;

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
