package dataengine.pipeline.model.sink;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = BatchSink.Builder.class)
public class BatchSink implements Sink, SinkWithFormat {

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
