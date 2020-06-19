package dataengine.model.pipeline.sink;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = SparkStreamForeachBatchSink.Builder.class)
public class SparkStreamForeachBatchSink implements Sink {

    @Nonnull
    String using;
    @Nonnull
    String name;
    @Nonnull
    Trigger trigger;
    @Nonnull
    SparkStreamSink.OutputMode mode;
    @Nonnull
    List<ForeachPipeline> batches;

}
