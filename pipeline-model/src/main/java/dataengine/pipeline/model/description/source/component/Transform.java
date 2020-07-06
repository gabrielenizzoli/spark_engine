package dataengine.pipeline.model.description.source.component;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.description.source.TransformationComponentWithSingleInput;
import dataengine.pipeline.model.description.source.Component;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Transform.Builder.class)
public class Transform implements Component, TransformationComponentWithSingleInput {

    @Nonnull
    String using;
    @Nonnull
    String withClass;

}
