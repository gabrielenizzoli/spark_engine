package sparkengine.plan.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import sparkengine.plan.model.component.ComponentWithNoInput;
import sparkengine.plan.model.component.ComponentWithSchema;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = InlineComponent.InlineComponentBuilder.class)
public class InlineComponent implements ComponentWithNoInput, ComponentWithSchema {

    public static final String TYPE_NAME = "inline";

    @Nullable
    @With
    List<Map<String, Object>> data;
    @Nullable
    String schema;

}
