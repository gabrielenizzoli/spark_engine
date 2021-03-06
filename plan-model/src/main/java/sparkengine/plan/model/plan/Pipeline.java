package sparkengine.plan.model.plan;


import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import lombok.With;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Pipeline.PipelineBuilder.class)
public class Pipeline {
    @Nullable
    @With
    Integer order;
    @Nonnull
    PipelineLayout layout;
}
