package sparkengine.plan.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.component.ComponentWithMultipleInputs;
import sparkengine.plan.model.encoder.DataEncoder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = TransformComponent.Builder.class)
public class TransformComponent implements ComponentWithMultipleInputs {

    public static final String TYPE_NAME = "transform";

    @Nullable
    List<String> using;
    @Nonnull
    String transformWith;
    @Nullable
    DataEncoder encodedAs;

}
