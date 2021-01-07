package dataengine.pipeline.model.source.component;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.source.Component;
import dataengine.pipeline.model.source.TransformationComponentWithMultipleInputs;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Union.Builder.class)
public class Union implements Component, TransformationComponentWithMultipleInputs {

    @Nonnull
    List<String> using;

}