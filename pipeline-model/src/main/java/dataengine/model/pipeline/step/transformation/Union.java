package dataengine.model.pipeline.step.transformation;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.model.pipeline.step.MultiInputStep;
import dataengine.model.pipeline.step.Step;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Union.Builder.class)
public class Union implements Step, MultiInputStep {

    @Nonnull
    List<String> using;

}
