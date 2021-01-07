package dataengine.pipeline.model.collection;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.sink.Sink;
import dataengine.pipeline.model.source.Component;
import lombok.Builder;
import lombok.Value;

import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Pipelines.Builder.class)
public class Pipelines {

    Map<String, Component> components;
    Map<String, Sink> sinks;
    Map<String, Pipeline> pipelines;

}
