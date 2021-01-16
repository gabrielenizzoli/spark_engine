package dataengine.pipeline.model.pipeline;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.sink.Sink;
import dataengine.pipeline.model.component.Component;
import lombok.Builder;
import lombok.Value;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Plan.Builder.class)
public class Plan {

    Map<String, Component> components;
    Map<String, Sink> sinks;
    List<Pipeline> pipelines;

}
