package dataengine.model.pipeline.sink;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.model.pipeline.step.Step;
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
    Map<String, Step> steps;
    @Nonnull
    Sink sink;

}
