package dataengine.pipeline.model.description.sink;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
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
