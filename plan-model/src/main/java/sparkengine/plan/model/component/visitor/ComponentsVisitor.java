package sparkengine.plan.model.component.visitor;

import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.component.Component;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;
import java.util.Map;

public class ComponentsVisitor {

    private ComponentsVisitor() {
    }

    public static void visitComponents(
            @Nonnull Location location,
            @Nonnull ComponentVisitor componentVisitor,
            @Nonnull Map<String, Component> components) throws Exception {

        for (var nameAndComponent : components.entrySet()) {

            var name = nameAndComponent.getKey();
            var component = nameAndComponent.getValue();
            visitComponent(location.push(name), componentVisitor, component);
        }

    }

    public static void visitComponent(@Nonnull Location location,
                                         @Nonnull ComponentVisitor componentVisitor,
                                         @Nonnull Component component) throws Exception {

        try {
            String name = String.format("visit%s", component.getClass().getSimpleName());
            Method visitor = componentVisitor.getClass().getMethod(name, Location.class, component.getClass());
            visitor.invoke(componentVisitor, location, component);
        } catch (SecurityException | NoSuchMethodException t) {
            throw new InternalVisitorError("unmanaged " + component.componentTypeName() + " component", t);
        } catch (Exception t) {
            throw new InternalVisitorError("issue resolving " + component.componentTypeName() + " component in location " + location, t);
        }
    }

    public static class InternalVisitorError extends Error {

        public InternalVisitorError(String str) {
            super(str);
        }

        public InternalVisitorError(String str, Throwable t) {
            super(str, t);
        }

    }

}
