package sparkengine.plan.model.sink.mapper;

import org.junit.jupiter.api.Test;
import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.sink.Sink;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SinkMapperTest {

    @Test
    public void testMapperForEachSinkIsDefinedInInterface() {

        Set<Method> allMethods = Arrays.stream(SinkMapper.class.getDeclaredMethods()).collect(Collectors.toSet());

        for (Class<Sink> sinkType : Sink.SINK_TYPE_MAP.keySet())  {

            String name = "map" + sinkType.getSimpleName();
            Method method = null;
            Throwable error = null;
            try {
                 method = SinkMapper.class.getDeclaredMethod(name, Location.class, sinkType);
                 allMethods.remove(method);
            } catch (Throwable t) {
                error = t;
            }
            assertNotNull(method, "can't locate method " + name + " for type " + sinkType + ": " + Optional.ofNullable(error).map(Throwable::getMessage).orElse(""));

        }

        assertTrue(allMethods.isEmpty(), "some methods in interface do not have a matching sink: " + allMethods);

    }

}