package sparkengine.plan.model.sink.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.sink.Sink;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = CounterSink.Builder.class)
public class CounterSink implements Sink {

    public static final String TYPE_NAME = "counter";

    @Nonnull
    String key;

}
