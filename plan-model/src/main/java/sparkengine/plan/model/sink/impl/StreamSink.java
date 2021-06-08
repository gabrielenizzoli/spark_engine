package sparkengine.plan.model.sink.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import sparkengine.plan.model.sink.SinkForStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = StreamSink.StreamSinkBuilder.class)
public class StreamSink implements SinkForStream {

    public static final String TYPE_NAME = "stream";

    @Nonnull
    String name;
    @Nonnull
    String format;
    @Nullable
    String checkpointLocation;
    @Nullable
    @With
    Map<String, String> options;
    @Nullable
    Trigger trigger;
    @Nullable
    OutputMode mode;

}
