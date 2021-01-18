package dataengine.pipeline.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.component.ComponentWithNoInput;
import dataengine.pipeline.model.component.EncodedComponent;
import dataengine.pipeline.model.encoder.DataEncoder;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = StreamComponent.Builder.class)
public class StreamComponent implements ComponentWithNoInput, EncodedComponent {

    @Nonnull
    String format;
    @Nullable
    Map<String, String> options;
    @Nullable
    DataEncoder encodedAs;

}
