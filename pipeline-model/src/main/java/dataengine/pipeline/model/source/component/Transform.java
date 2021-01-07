package dataengine.pipeline.model.source.component;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.encoder.DataEncoder;
import dataengine.pipeline.model.source.Component;
import dataengine.pipeline.model.source.TransformationComponentWithMultipleInputs;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Transform.Builder.class)
public class Transform implements Component, TransformationComponentWithMultipleInputs {

    @Nonnull
    @lombok.Builder.Default
    List<String> using = List.of();
    @Nonnull
    String with;
    @Nullable
    DataEncoder encodedAs;

}
