package sparkengine.plan.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.component.ComponentWithSingleInput;
import sparkengine.plan.model.encoder.DataEncoder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = MapComponent.MapComponentBuilder.class)
public class MapComponent implements ComponentWithSingleInput {

    public static final String TYPE_NAME = "map";

    @Nullable
    String using;
    @Nullable
    Map<String, String> accumulators;
    @Nullable
    Map<String, Object> params;
    @Nonnull
    String transformWith;
    @Nullable
    DataEncoder encodedAs;

}
