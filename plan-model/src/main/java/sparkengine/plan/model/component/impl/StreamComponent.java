package sparkengine.plan.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import sparkengine.plan.model.component.ComponentWithEncoder;
import sparkengine.plan.model.component.ComponentWithNoInput;
import sparkengine.plan.model.encoder.DataEncoder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = StreamComponent.StreamComponentBuilder.class)
public class StreamComponent implements ComponentWithNoInput, ComponentWithEncoder {

    public static final String TYPE_NAME = "stream";

    @Nonnull
    String format;
    @Nullable
    @With
    Map<String, String> options;
    @Nullable
    DataEncoder encodedAs;

}
