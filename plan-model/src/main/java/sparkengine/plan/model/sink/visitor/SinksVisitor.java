package sparkengine.plan.model.sink.visitor;

import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.sink.Sink;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;
import java.util.Map;

public class SinksVisitor {

    private SinksVisitor() {
    }

    public static void visitSinks(
            @Nonnull Location location,
            @Nonnull SinkVisitor sinkVisitor,
            @Nonnull Map<String, Sink> sinks) throws Exception {

        for (var nameAndSink : sinks.entrySet()) {

            var name = nameAndSink.getKey();
            var component = nameAndSink.getValue();
            visitSink(location.push(name), sinkVisitor, component);
        }
    }

    public static void visitSink(
            @Nonnull Location location,
            @Nonnull SinkVisitor sinkVisitor,
            @Nonnull Sink sink) throws Exception {

        try {
            String name = String.format("visit%s", sink.getClass().getSimpleName());
            Method mapper = sinkVisitor.getClass().getMethod(name, Location.class, sink.getClass());
            mapper.invoke(sinkVisitor, location, sink);
        } catch (SecurityException | NoSuchMethodException t) {
            throw new InternalVisitorError("unmanaged " + sink.sinkTypeName() + " sink", t);
        } catch (Exception t) {
            throw new InternalVisitorError("issue resolving " + sink.sinkTypeName() + " sink", t);
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
