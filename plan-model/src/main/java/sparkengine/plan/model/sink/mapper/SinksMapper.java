package sparkengine.plan.model.sink.mapper;

import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.sink.Sink;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

public class SinksMapper {

    private SinksMapper() {
    }

    public static Map<String, Sink> mapSinks(
            @Nonnull Location location,
            @Nonnull SinkMapper sinkMapper,
            @Nonnull Map<String, Sink> sinks) throws Exception {

        var newSinks = new LinkedHashMap<String, Sink>();
        for (var nameAndSink : sinks.entrySet()) {

            var name = nameAndSink.getKey();
            var component = nameAndSink.getValue();
            var newSink = mapSink(location.push(name), sinkMapper, component);

            newSinks.put(name, newSink);
        }

        return newSinks;
    }

    public static Sink mapSink(
            @Nonnull Location location,
            @Nonnull SinkMapper sinkMapper,
            @Nonnull Sink sink) throws Exception {

        try {
            String name = String.format("map%s", sink.getClass().getSimpleName());
            Method mapper = sinkMapper.getClass().getMethod(name, Location.class, sink.getClass());
            Object returnedSink = mapper.invoke(sinkMapper, location, sink);
            return (Sink)returnedSink;
        } catch (SecurityException | NoSuchMethodException t) {
            throw new InternalMapperError("unmanaged " + sink.sinkTypeName() + " sink", t);
        } catch (Exception t) {
            throw new InternalMapperError("issue resolving " + sink.sinkTypeName() + " sink", t);
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
