package dataengine.pipeline.model.source.component;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.source.SchemaComponent;
import dataengine.pipeline.model.source.TransformationComponentWithMultipleInputs;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = PlaceholderSchemaComponent.Builder.class)
@Deprecated
public class PlaceholderSchemaComponent implements SchemaComponent, TransformationComponentWithMultipleInputs {

    @Nonnull
    String schema;
    @Nonnull
    List<String> using;

}
