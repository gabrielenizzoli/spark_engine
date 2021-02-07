package sparkengine.plan.model.sink.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import sparkengine.plan.model.plan.Plan;
import sparkengine.plan.model.sink.Sink;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = ForeachSink.Builder.class)
public class ForeachSink implements Sink {

    public static final String TYPE_NAME = "foreach";

    @Nonnull
    String name;
    @Nullable
    Map<String, String> options;
    @Nonnull
    Trigger trigger;
    @Nonnull
    StreamSink.OutputMode mode;
    @Nonnull
    String batchComponentName;
    @Nonnull
    @With
    Plan plan;

}
