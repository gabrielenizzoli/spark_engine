package sparkengine.plan.model.sink.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.sink.Sink;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = ShowSink.ShowSinkBuilder.class)
public class ShowSink implements Sink {

    public static final String TYPE_NAME = "show";

    @lombok.Builder.Default
    int numRows = 20;
    @lombok.Builder.Default
    int truncate = 30;

}
