package dataengine.pipeline.model.pipeline.step.source;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.pipeline.encoder.Encoder;
import dataengine.pipeline.model.pipeline.step.Source;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = SparkSqlSource.Builder.class)
public class SparkSqlSource implements Source {

    @Nonnull
    String sql;
    @Nullable
    Encoder as;

}
