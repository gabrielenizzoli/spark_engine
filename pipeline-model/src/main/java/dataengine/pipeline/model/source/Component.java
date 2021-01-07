package dataengine.pipeline.model.source;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import dataengine.pipeline.model.source.component.*;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        // sources
        @JsonSubTypes.Type(value = EmptySource.class, name = "emptySource"),
        @JsonSubTypes.Type(value = InlineSource.class, name = "inlineSource"),
        @JsonSubTypes.Type(value = BatchSource.class, name = "batchSource"),
        @JsonSubTypes.Type(value = SqlSource.class, name = "sqlSource"), // deprectaed
        @JsonSubTypes.Type(value = StreamSource.class, name = "streamSource"),

        // transformation with single input
        @JsonSubTypes.Type(value = Encode.class, name = "encode"),

        // transformation with many inputs
        @JsonSubTypes.Type(value = Transform.class, name = "transform"),
        @JsonSubTypes.Type(value = Union.class, name = "union"),
        @JsonSubTypes.Type(value = Sql.class, name = "sql"),

        // placeholders
        @JsonSubTypes.Type(value = PlaceholderSchemaComponent.class, name = "placeholder")
})
public interface Component {

}
