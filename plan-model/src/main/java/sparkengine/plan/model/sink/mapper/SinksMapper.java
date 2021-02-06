package sparkengine.plan.model.sink.mapper;

import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.impl.*;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

public class SinksMapper {

    private SinksMapper() {
    }

    public static Map<String, Sink> mapSinks(
            @Nonnull Stack<String> location,
            @Nonnull SinkMapper sinkMapper,
            @Nonnull Map<String, Sink> sinks) throws Exception {

        var newSinks = new LinkedHashMap<String, Sink>();
        for (var nameAndSink : sinks.entrySet()) {

            var name = nameAndSink.getKey();
            var component = nameAndSink.getValue();
            location.push(name);
            var newSink = mapSink(location, sinkMapper, component);
            location.pop();

            newSinks.put(name, newSink);
        }

        return newSinks;
    }

    public static Sink mapSink(
            @Nonnull Stack<String> location,
            @Nonnull SinkMapper sinkMapper,
            @Nonnull Sink sink) throws Exception {

        try {
            if (sink instanceof ShowSink)
                return sinkMapper.mapShowSink(location, (ShowSink) sink);
            if (sink instanceof ViewSink)
                return sinkMapper.mapViewSink(location, (ViewSink) sink);
            if (sink instanceof CounterSink)
                return sinkMapper.mapCounterSink(location, (CounterSink) sink);
            if (sink instanceof BatchSink)
                return sinkMapper.mapBatchSink(location, (BatchSink) sink);
            if (sink instanceof StreamSink)
                return sinkMapper.mapStreamSink(location, (StreamSink) sink);
            if (sink instanceof ForeachSink)
                return sinkMapper.mapForeachSink(location, (ForeachSink) sink);
            if (sink instanceof ReferenceSink)
                return sinkMapper.mapReferenceSink(location, (ReferenceSink) sink);
        } catch (Exception t) {
            throw new InternalMapperError("issue resolving " + sink.sinkTypeName() + " sink", t);
        }

        throw new InternalMapperError("unmanaged " + sink.sinkTypeName() + " sink");
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
