package dataengine.pipeline.model.sink.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.sink.Sink;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = StreamSink.Builder.class)
public class StreamSink implements Sink, SinkWithFormat {

    @Nonnull
    String name;
    @Nonnull
    String format;
    @Nullable
    String checkpointLocation;
    @Nullable
    Map<String, String> options;
    @Nullable
    List<String> partitionColumns;
    @Nullable
    Trigger trigger;
    @Nullable
    OutputMode mode;

    public enum OutputMode {
        APPEND,
        COMPLETE,
        UPDATE
    }

}
