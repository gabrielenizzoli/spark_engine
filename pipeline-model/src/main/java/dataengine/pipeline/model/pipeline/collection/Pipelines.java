package dataengine.pipeline.model.pipeline.collection;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.pipeline.sink.Sink;
import dataengine.pipeline.model.pipeline.step.Step;
import lombok.Builder;
import lombok.Value;

import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Pipelines.Builder.class)
public class Pipelines {

    Map<String, Step> steps;
    Map<String, Sink> sinks;

}
