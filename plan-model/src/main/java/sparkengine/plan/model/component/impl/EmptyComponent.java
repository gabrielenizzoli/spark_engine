package sparkengine.plan.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.component.ComponentWithNoInput;
import sparkengine.plan.model.component.EncodedComponent;
import sparkengine.plan.model.encoder.DataEncoder;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = EmptyComponent.Builder.class)
public class EmptyComponent implements ComponentWithNoInput, EncodedComponent {

    public static final String TYPE_NAME = "empty";

    @Nonnull
    DataEncoder encodedAs;

}
