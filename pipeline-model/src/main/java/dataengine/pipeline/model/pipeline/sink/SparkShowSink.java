package dataengine.pipeline.model.pipeline.sink;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = SparkShowSink.Builder.class)
public class SparkShowSink implements Sink {

    @Nonnull
    String using;
    @lombok.Builder.Default
    int numRows = 20;
    @lombok.Builder.Default
    int truncate = 30;

}
