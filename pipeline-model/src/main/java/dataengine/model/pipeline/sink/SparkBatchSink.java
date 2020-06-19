package dataengine.model.pipeline.sink;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = SparkBatchSink.Builder.class)
public class SparkBatchSink implements Sink {

    @Nonnull
    String using;
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
