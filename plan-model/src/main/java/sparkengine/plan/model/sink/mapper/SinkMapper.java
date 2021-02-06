package sparkengine.plan.model.sink.mapper;

import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.impl.*;

import java.util.Stack;

public interface SinkMapper {

    default Sink mapShowSink(Stack<String> location, ShowSink sink) throws Exception {
        return sink;
    }

    default Sink mapViewSink(Stack<String> location, ViewSink sink) throws Exception {
        return sink;
    }

    default Sink mapCounterSink(Stack<String> location, CounterSink sink) throws Exception {
        return sink;
    }

    default Sink mapBatchSink(Stack<String> location, BatchSink sink) throws Exception {
        return sink;
    }

    default Sink mapStreamSink(Stack<String> location, StreamSink sink) throws Exception {
        return sink;
    }

    default Sink mapForeachSink(Stack<String> location, ForeachSink sink) throws Exception {
        return sink;
    }

    default Sink mapReferenceSink(Stack<String> location, ReferenceSink sink) throws Exception {
        return sink;
    }

}
