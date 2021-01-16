package dataengine.pipeline.model.sink.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.sink.Sink;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = CounterSink.Builder.class)
public class CounterSink implements Sink {

    @Nonnull
    String key;

}
