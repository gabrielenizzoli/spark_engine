package sparkengine.plan.model.plan;


import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Pipeline.PipelineBuilder.class)
public class Pipeline {
    String component;
    String sink;
}
