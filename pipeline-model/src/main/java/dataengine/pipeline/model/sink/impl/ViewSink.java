package dataengine.pipeline.model.sink.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.sink.Sink;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = ViewSink.Builder.class)
public class ViewSink implements Sink {

    @Nonnull
    String name;

}
