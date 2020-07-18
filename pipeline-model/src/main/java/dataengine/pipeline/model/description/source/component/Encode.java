package dataengine.pipeline.model.description.source.component;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.description.encoder.DataEncoder;
import dataengine.pipeline.model.description.source.Component;
import dataengine.pipeline.model.description.source.EncodedComponent;
import dataengine.pipeline.model.description.source.TransformationComponentWithSingleInput;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Encode.Builder.class)
public class Encode implements Component, TransformationComponentWithSingleInput, EncodedComponent {

    @Nonnull
    String using;
    @Nonnull
    DataEncoder encodedAs;

}
