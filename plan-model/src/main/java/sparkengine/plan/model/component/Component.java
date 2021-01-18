package sparkengine.plan.model.component;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import sparkengine.plan.model.component.impl.*;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        // sources
        @JsonSubTypes.Type(value = EmptyComponent.class, name = "empty"),
        @JsonSubTypes.Type(value = InlineComponent.class, name = "inline"),
        @JsonSubTypes.Type(value = BatchComponent.class, name = "batch"),
        @JsonSubTypes.Type(value = StreamComponent.class, name = "stream"),

        // transformation with single input
        @JsonSubTypes.Type(value = EncodeComponent.class, name = "encode"),

        // transformation with many inputs
        @JsonSubTypes.Type(value = TransformComponent.class, name = "transform"),
        @JsonSubTypes.Type(value = UnionComponent.class, name = "union"),
        @JsonSubTypes.Type(value = SqlComponent.class, name = "sql")
})
public interface Component {

}
