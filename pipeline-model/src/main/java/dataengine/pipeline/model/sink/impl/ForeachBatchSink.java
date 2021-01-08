package dataengine.pipeline.model.sink.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.sink.Sink;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = ForeachBatchSink.Builder.class)
public class ForeachBatchSink implements Sink {

    @Nonnull
    String name;
    @Nonnull
    Trigger trigger;
    @Nonnull
    StreamSink.OutputMode mode;
    @Nonnull
    List<ForeachPipeline> batches;

}
