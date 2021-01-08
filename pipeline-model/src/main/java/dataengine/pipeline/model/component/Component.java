package dataengine.pipeline.model.component;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import dataengine.pipeline.model.component.impl.*;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        // sources
        @JsonSubTypes.Type(value = EmptyComponent.class, name = "emptySource"),
        @JsonSubTypes.Type(value = InlineComponent.class, name = "inlineSource"),
        @JsonSubTypes.Type(value = BatchComponent.class, name = "batchSource"),
        @JsonSubTypes.Type(value = StreamComponent.class, name = "streamSource"),

        // transformation with single input
        @JsonSubTypes.Type(value = EncodeComponent.class, name = "encode"),

        // transformation with many inputs
        @JsonSubTypes.Type(value = TransformComponent.class, name = "transform"),
        @JsonSubTypes.Type(value = UnionComponent.class, name = "union"),
        @JsonSubTypes.Type(value = SqlComponent.class, name = "sql")
})
public interface Component {

}
