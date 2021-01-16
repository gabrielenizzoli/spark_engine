package dataengine.pipeline.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.component.ComponentWithNoInput;
import lombok.Builder;
import lombok.Value;

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
