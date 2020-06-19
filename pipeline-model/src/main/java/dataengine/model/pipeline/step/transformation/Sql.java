package dataengine.model.pipeline.step.transformation;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.model.pipeline.step.SingleInput;
import dataengine.model.pipeline.step.Step;
import dataengine.model.pipeline.step.source.SparkBatchSource;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Sql.Builder.class)
public class Sql implements Step, SingleInput {

    @Nonnull
    String using;
    @Nonnull
    String sql;

}