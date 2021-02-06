package sparkengine.plan.model.sink.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.sink.SinkWithFormat;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = BatchSink.Builder.class)
public class BatchSink implements SinkWithFormat {

    public static final String TYPE_NAME = "batch";

    @Nonnull
    String format;
    @Nullable
    Map<String, String> options;
    @Nullable
    List<String> partitionColumns;
    @Nullable
    WriteMode mode;

    public enum WriteMode {
        APPEND,
        OVERWRITE,
        ERROR_IF_EXISTS,
        IGNORE
    }

}
