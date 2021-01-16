package dataengine.pipeline.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.encoder.DataEncoder;
import dataengine.pipeline.model.component.EncodedComponent;
import dataengine.pipeline.model.component.ComponentWithNoInput;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = BatchComponent.Builder.class)
public class BatchComponent implements ComponentWithNoInput, EncodedComponent {

    @Nonnull
    String format;
    @Nonnull
    Map<String, String> options;
    @Nullable
    DataEncoder encodedAs;

}
