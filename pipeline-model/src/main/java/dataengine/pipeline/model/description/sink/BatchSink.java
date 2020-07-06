package dataengine.pipeline.model.description.sink;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = BatchSink.Builder.class)
public class BatchSink implements Sink {

    @Nonnull
    String format;
    @Nonnull
    Map<String, String> options;
    @Nonnull
    @lombok.Builder.Default
    WriteMode mode = WriteMode.ERROR_IF_EXISTS;

    public enum WriteMode {
        APPEND,
        OVERWRITE,
        ERROR_IF_EXISTS,
        IGNORE
    }

}
