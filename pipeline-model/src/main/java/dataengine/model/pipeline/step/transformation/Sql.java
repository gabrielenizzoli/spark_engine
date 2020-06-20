package dataengine.model.pipeline.step.transformation;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.model.pipeline.step.SingleInputStep;
import dataengine.model.pipeline.step.Step;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Sql.Builder.class)
public class Sql implements Step, SingleInputStep {

    @Nonnull
    String using;
    @Nonnull
    String sql;

}
