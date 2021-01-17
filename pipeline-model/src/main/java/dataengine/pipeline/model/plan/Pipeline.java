package dataengine.pipeline.model.plan;


import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Pipeline.Builder.class)
public class Pipeline {
    String source;
    String sink;
}
