package dataengine.pipeline.model.sink;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = BatchSink.Builder.class)
public class CollectSink implements Sink {

    @lombok.Builder.Default
    int limit = 100;

}
