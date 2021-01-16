package dataengine.pipeline.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.component.Component;
import dataengine.pipeline.model.component.ComponentWithMultipleInputs;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = UnionComponent.Builder.class)
public class UnionComponent implements Component, ComponentWithMultipleInputs {

    @Nullable
    List<String> using;

}
