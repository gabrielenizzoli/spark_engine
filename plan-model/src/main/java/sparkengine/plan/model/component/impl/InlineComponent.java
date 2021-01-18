package sparkengine.plan.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.component.ComponentWithNoInput;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = InlineComponent.Builder.class)
public class InlineComponent implements ComponentWithNoInput {

    @Nullable
    List<Map<String, Object>> data;
    @Nullable
    String schema;

}
