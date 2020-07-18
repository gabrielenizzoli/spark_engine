package dataengine.pipeline.model.description.source.component;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.description.encoder.DataEncoder;
import dataengine.pipeline.model.description.source.Component;
import dataengine.pipeline.model.description.source.TransformationComponentWithMultipleInputs;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Merge.Builder.class)
public class Merge implements Component, TransformationComponentWithMultipleInputs {

    @Nonnull
    List<String> using;
    @Nonnull
    String withClass;
    @Nullable
    DataEncoder encodedAs;

}
