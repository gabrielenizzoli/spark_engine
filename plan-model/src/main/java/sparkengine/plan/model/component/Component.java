package sparkengine.plan.model.component;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import sparkengine.plan.model.component.impl.*;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = SqlComponent.class)
@JsonSubTypes({
        // sources
        @JsonSubTypes.Type(value = EmptyComponent.class, name = EmptyComponent.TYPE_NAME),
        @JsonSubTypes.Type(value = InlineComponent.class, name = InlineComponent.TYPE_NAME),
        @JsonSubTypes.Type(value = BatchComponent.class, name = BatchComponent.TYPE_NAME),
        @JsonSubTypes.Type(value = StreamComponent.class, name = StreamComponent.TYPE_NAME),

        // transformation with single input
        @JsonSubTypes.Type(value = EncodeComponent.class, name = EncodeComponent.TYPE_NAME),

        // transformation with many inputs
        @JsonSubTypes.Type(value = TransformComponent.class, name = TransformComponent.TYPE_NAME),
        @JsonSubTypes.Type(value = UnionComponent.class, name = UnionComponent.TYPE_NAME),
        @JsonSubTypes.Type(value = FragmentComponent.class, name = FragmentComponent.TYPE_NAME),
        @JsonSubTypes.Type(value = WrapperComponent.class, name = WrapperComponent.TYPE_NAME),
        @JsonSubTypes.Type(value = SqlComponent.class, name = SqlComponent.TYPE_NAME),
        @JsonSubTypes.Type(value = ReferenceComponent.class, name = ReferenceComponent.TYPE_NAME)
})
public interface Component {

    // map of components by type
    Map<Class<Component>, String> COMPONENT_TYPE_MAP = Arrays
            .stream(Component.class.getAnnotation(JsonSubTypes.class).value())
            .collect(Collectors.toMap(t -> (Class<Component>) t.value(), JsonSubTypes.Type::name));

    // map of components by name
    Map<String, Class<Component>> COMPONENT_NAME_MAP = Arrays
            .stream(Component.class.getAnnotation(JsonSubTypes.class).value())
            .collect(Collectors.toMap(JsonSubTypes.Type::name, t -> (Class<Component>) t.value()));

    default String componentTypeName() {
        return COMPONENT_TYPE_MAP.get(this.getClass());
    }

}
