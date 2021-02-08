package sparkengine.plan.model.component.mapper;

import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.impl.*;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;

public class ComponentsMapper {

    public static final String WRAPPER = "wrapper";

    private ComponentsMapper() {
    }

    public static Map<String, Component> mapComponents(
            @Nonnull Location location,
            @Nonnull ComponentMapper componentMapper,
            @Nonnull Map<String, Component> components) throws Exception {

        var newComponents = new LinkedHashMap<String, Component>();
        for (var nameAndComponent : components.entrySet()) {

            var name = nameAndComponent.getKey();
            var component = nameAndComponent.getValue();
            var newComponent = mapComponent(location.push(name), componentMapper, component);
            newComponents.put(name, newComponent);
        }

        return newComponents;
    }

    public static Component mapComponent(@Nonnull Location location,
                                         @Nonnull ComponentMapper componentMapper,
                                         @Nonnull Component component) throws Exception {

        try {
            if (component instanceof EmptyComponent)
                return componentMapper.mapEmptyComponent(location, (EmptyComponent) component);
            if (component instanceof InlineComponent)
                return componentMapper.mapInlineComponent(location, (InlineComponent) component);
            if (component instanceof BatchComponent)
                return componentMapper.mapBatchComponent(location, (BatchComponent) component);
            if (component instanceof StreamComponent)
                return componentMapper.mapStreamComponent(location, (StreamComponent) component);
            if (component instanceof SchemaValidationComponent)
                return componentMapper.mapSchemaValidationComponent(location, (SchemaValidationComponent) component);
            if (component instanceof EncodeComponent)
                return componentMapper.mapEncodeComponent(location, (EncodeComponent) component);
            if (component instanceof TransformComponent)
                return componentMapper.mapTransformComponent(location, (TransformComponent) component);
            if (component instanceof UnionComponent)
                return componentMapper.mapUnionComponent(location, (UnionComponent) component);
            if (component instanceof FragmentComponent) {
                FragmentComponent fragmentComponent = (FragmentComponent) component;
                fragmentComponent = fragmentComponent.withComponents(mapComponents(location, componentMapper, fragmentComponent.getComponents()));
                return componentMapper.mapFragmentComponent(location, fragmentComponent);
            }
            if (component instanceof WrapperComponent) {
                WrapperComponent wrapperComponent = (WrapperComponent) component;
                wrapperComponent = wrapperComponent.withComponent(mapComponent(location.push(WRAPPER), componentMapper, wrapperComponent.getComponent()));
                return componentMapper.mapWrapperComponent(location, wrapperComponent);
            }
            if (component instanceof SqlComponent) {
                return componentMapper.mapSqlComponent(location, (SqlComponent) component);
            }
            if (component instanceof ReferenceComponent) {
                return componentMapper.mapReferenceComponent(location, (ReferenceComponent) component);
            }
        } catch (Exception t) {
            throw new InternalMapperError("issue resolving " + component.componentTypeName() + " component in location " + location, t);
        }

        throw new InternalMapperError("unmanaged " + component.componentTypeName() + " component in location " + location);
    }

    public static class InternalMapperError extends Error {

        public InternalMapperError(String str) {
            super(str);
        }

        public InternalMapperError(String str, Throwable t) {
            super(str, t);
        }

    }

}
