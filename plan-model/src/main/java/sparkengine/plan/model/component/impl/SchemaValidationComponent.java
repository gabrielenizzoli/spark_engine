package sparkengine.plan.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.component.ComponentWithSchema;
import sparkengine.plan.model.component.ComponentWithSingleInput;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = SchemaValidationComponent.Builder.class)
public class SchemaValidationComponent implements ComponentWithSingleInput, ComponentWithSchema {

    public static final String TYPE_NAME = "schemaTest";

    @Nonnull
    String using;
    @Nonnull
    String schema;

}
