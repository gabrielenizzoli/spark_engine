package dataengine.pipeline.model.description.sink;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = StreamSink.Builder.class)
public class StreamSink implements Sink {

    @Nonnull
    String name;
    @Nonnull
    String format;
    @Nonnull
    Map<String, String> options;
    @Nonnull
    Trigger trigger;
    @Nonnull
    OutputMode mode;

    public enum OutputMode {
        APPEND,
        COMPLETE,
        UPDATE
    }

}
