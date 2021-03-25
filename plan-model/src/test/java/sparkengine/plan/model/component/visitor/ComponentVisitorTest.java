package sparkengine.plan.model.component.visitor;

import org.junit.jupiter.api.Test;
import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.mapper.ComponentMapper;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ComponentVisitorTest {

    @Test
    public void testVisitorForEachComponentIsDefinedInInterface() {

        Set<Method> allMethods = Arrays.stream(ComponentVisitor.class.getDeclaredMethods()).collect(Collectors.toSet());

        for (Class<Component> componentType : Component.COMPONENT_TYPE_MAP.keySet())  {

            String name = "visit" + componentType.getSimpleName();
            Method method = null;
            Throwable error = null;
            try {
                method = ComponentVisitor.class.getDeclaredMethod(name, Location.class, componentType);
                allMethods.remove(method);
            } catch (Throwable t) {
                error = t;
            }
            assertNotNull(method, "can't locate method " + name + " for type " + componentType + ": " + Optional.ofNullable(error).map(Throwable::getMessage).orElse(""));

        }

        assertTrue(allMethods.isEmpty(), "some methods in interface do not have a matching component: " + allMethods);

    }

}