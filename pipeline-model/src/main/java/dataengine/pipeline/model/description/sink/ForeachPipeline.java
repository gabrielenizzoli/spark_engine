package dataengine.pipeline.model.description.sink;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.description.source.Component;
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
