package dataengine.model.pipeline.step.source;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.model.pipeline.encoder.Encoder;
import dataengine.model.pipeline.step.Source;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = SparkStreamSource.Builder.class)
public class SparkStreamSource implements Source {

    @Nonnull
    String format;
    @Nonnull
    Map<String, String> options;
    @Nullable
    Encoder as;

}
