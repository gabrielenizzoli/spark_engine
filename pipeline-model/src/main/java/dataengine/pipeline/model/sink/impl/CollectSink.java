package dataengine.pipeline.model.sink.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.sink.Sink;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = CollectSink.Builder.class)
public class CollectSink implements Sink {

    @lombok.Builder.Default
    int limit = 100;

}
