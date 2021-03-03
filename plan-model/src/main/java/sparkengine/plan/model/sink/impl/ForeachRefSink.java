package sparkengine.plan.model.sink.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.sink.SinkForStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = ForeachRefSink.Builder.class)
public class ForeachRefSink implements SinkForStream {

    public static final String TYPE_NAME = "foreachRef";

    @Nonnull
    String name;
    @Nonnull
    @lombok.Builder.Default
    String format = "foreach";
    @Nullable
    String checkpointLocation;
    @Nullable
    Map<String, String> options;
    @Nullable
    Trigger trigger;
    @Nullable
    OutputMode mode;

    @Nonnull
    String batchComponentName;
    @Nonnull
    String ref;

}
