package sparkengine.plan.model.sink.mapper;

import org.junit.jupiter.api.Test;
import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.sink.Sink;

import java.lang.reflect.Method;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class SinkMapperTest {

    @Test
    public void testMapperForEachSinkIsDefinedInInterface() {

        for (Class<Sink> sinkType : Sink.SINK_TYPE_MAP.keySet())  {

            String name = "map" + sinkType.getSimpleName();
            Method method = null;
            Throwable error = null;
            try {
                 method = SinkMapper.class.getDeclaredMethod(name, Location.class, sinkType);
            } catch (Throwable t) {
                error = t;
            }
            assertNotNull(method, "can't locate method " + name + " for type " + sinkType + ": " + Optional.ofNullable(error).map(Throwable::getMessage).orElse(""));

        }

    }

}