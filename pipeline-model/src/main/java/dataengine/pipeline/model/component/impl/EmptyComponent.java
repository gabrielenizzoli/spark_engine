package dataengine.pipeline.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.component.ComponentWithNoInput;
import dataengine.pipeline.model.component.EncodedComponent;
import dataengine.pipeline.model.encoder.DataEncoder;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = EmptyComponent.Builder.class)
public class EmptyComponent implements ComponentWithNoInput, EncodedComponent {

    @Nonnull
    DataEncoder encodedAs;

}
