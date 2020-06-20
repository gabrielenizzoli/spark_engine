package dataengine.model.pipeline.step.transformation;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.model.pipeline.encoder.Encoder;
import dataengine.model.pipeline.step.SingleInputStep;
import dataengine.model.pipeline.step.Step;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Encode.Builder.class)
public class Encode implements Step, SingleInputStep {

    @Nonnull
    String using;
    @Nonnull
    Encoder as;

}
