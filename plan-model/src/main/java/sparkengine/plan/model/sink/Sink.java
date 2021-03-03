package sparkengine.plan.model.sink;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import sparkengine.plan.model.sink.impl.*;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = BatchSink.class, name = BatchSink.TYPE_NAME),
        @JsonSubTypes.Type(value = StreamSink.class, name = StreamSink.TYPE_NAME),
        @JsonSubTypes.Type(value = ForeachSink.class, name = ForeachSink.TYPE_NAME),
        @JsonSubTypes.Type(value = ShowSink.class, name = ShowSink.TYPE_NAME),
        @JsonSubTypes.Type(value = ViewSink.class, name = ViewSink.TYPE_NAME),
        @JsonSubTypes.Type(value = CounterSink.class, name = CounterSink.TYPE_NAME),
        @JsonSubTypes.Type(value = ReferenceSink.class, name = ReferenceSink.TYPE_NAME)
})
public interface Sink {

    // map of components by type
    Map<Class<Sink>, String> SINK_TYPE_MAP = Arrays
            .stream(Sink.class.getAnnotation(JsonSubTypes.class).value())
            .collect(Collectors.toMap(t -> (Class<Sink>) t.value(), JsonSubTypes.Type::name));

    // map of components by name
    Map<String, Class<Sink>> SINK_NAME_MAP = Arrays
            .stream(Sink.class.getAnnotation(JsonSubTypes.class).value())
            .collect(Collectors.toMap(JsonSubTypes.Type::name, t -> (Class<Sink>) t.value()));

    default String sinkTypeName() {
        return SINK_TYPE_MAP.get(this.getClass());
    }

}
