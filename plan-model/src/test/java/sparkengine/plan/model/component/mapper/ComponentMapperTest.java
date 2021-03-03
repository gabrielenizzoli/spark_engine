package sparkengine.plan.model.component.mapper;

import org.junit.jupiter.api.Test;
import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.component.Component;

import java.lang.reflect.Method;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class ComponentMapperTest {

    @Test
    public void testMapperForEachComponentIsDefinedInInterface() {

        for (Class<Component> componentType : Component.COMPONENT_TYPE_MAP.keySet())  {

            String name = "map" + componentType.getSimpleName();
            Method method = null;
            Throwable error = null;
            try {
                method = ComponentMapper.class.getDeclaredMethod(name, Location.class, componentType);
            } catch (Throwable t) {
                error = t;
            }
            assertNotNull(method, "can't locate method " + name + " for type " + componentType + ": " + Optional.ofNullable(error).map(Throwable::getMessage).orElse(""));

        }

    }

}