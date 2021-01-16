package dataengine.pipeline.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.encoder.DataEncoder;
import dataengine.pipeline.model.component.Component;
import dataengine.pipeline.model.component.EncodedComponent;
import dataengine.pipeline.model.component.ComponentWithSingleInput;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = EncodeComponent.Builder.class)
public class EncodeComponent implements Component, ComponentWithSingleInput, EncodedComponent {

    @Nonnull
    String using;
    @Nonnull
    DataEncoder encodedAs;

}
