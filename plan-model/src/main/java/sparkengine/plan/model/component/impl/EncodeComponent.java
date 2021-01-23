package sparkengine.plan.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.ComponentWithSingleInput;
import sparkengine.plan.model.component.EncodedComponent;
import sparkengine.plan.model.encoder.DataEncoder;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = EncodeComponent.Builder.class)
public class EncodeComponent implements ComponentWithSingleInput, EncodedComponent {

    public static final String TYPE_NAME = "encode";

    @Nonnull
    String using;
    @Nonnull
    DataEncoder encodedAs;

}
