package sparkengine.plan.model.sink.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.sink.Sink;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = ShowSink.Builder.class)
public class ShowSink implements Sink {

    @lombok.Builder.Default
    int numRows = 20;
    @lombok.Builder.Default
    int truncate = 30;

}
