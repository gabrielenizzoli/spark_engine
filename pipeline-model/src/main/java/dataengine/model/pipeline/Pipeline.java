package dataengine.model.pipeline;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.model.pipeline.sink.Sink;
import dataengine.model.pipeline.step.Step;
import lombok.Builder;
import lombok.Value;

import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Pipeline.Builder.class)
public class Pipeline {

    Map<String, Step> steps;
    Map<String, Sink> sinks;

}
