package dataengine.pipeline.model.source.component;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.encoder.DataEncoder;
import dataengine.pipeline.model.source.Component;
import dataengine.pipeline.model.source.EncodedComponent;
import dataengine.pipeline.model.source.TransformationComponentWithSingleInput;
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
