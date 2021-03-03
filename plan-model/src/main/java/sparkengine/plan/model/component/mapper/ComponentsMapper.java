package sparkengine.plan.model.component.mapper;

import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.sink.mapper.SinksMapper;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

public class ComponentsMapper {

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
            String name = "map" + component.getClass().getSimpleName();
            Method mapper = componentMapper.getClass().getMethod(name, Location.class, component.getClass());
            Object returnedComponent = mapper.invoke(componentMapper, location, component);
            return (Component) returnedComponent;
        } catch (SecurityException | NoSuchMethodException t) {
            throw new SinksMapper.InternalMapperError("unmanaged " + component.componentTypeName() + " component", t);
        } catch (Exception t) {
            throw new InternalMapperError("issue resolving " + component.componentTypeName() + " component in location " + location, t);
        }
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
