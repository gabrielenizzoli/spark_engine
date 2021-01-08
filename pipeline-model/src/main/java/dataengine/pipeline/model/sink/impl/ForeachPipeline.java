package dataengine.pipeline.model.sink.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.component.Component;
import dataengine.pipeline.model.sink.Sink;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = ForeachPipeline.Builder.class)
public class ForeachPipeline {

    @Nonnull
    String source;
    @Nonnull
    Map<String, Component> components;
    @Nonnull
    Sink sink;

}
