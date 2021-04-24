package sparkengine.plan.model.plan;


import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@AllArgsConstructor
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = PipelineLayout.PipelineLayoutBuilder.class)
public class PipelineLayout {

    @Nonnull
    String component;
    @Nonnull
    String sink;

}
