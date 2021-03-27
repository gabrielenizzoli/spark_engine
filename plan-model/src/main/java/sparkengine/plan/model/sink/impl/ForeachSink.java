package sparkengine.plan.model.sink.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import sparkengine.plan.model.plan.Plan;
import sparkengine.plan.model.sink.SinkForStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = ForeachSink.ForeachSinkBuilder.class)
public class ForeachSink implements SinkForStream {

    public static final String TYPE_NAME = "foreach";

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
    StreamSink.OutputMode mode;

    @Nonnull
    String batchComponentName;
    @Nonnull
    @With
    Plan plan;

}
