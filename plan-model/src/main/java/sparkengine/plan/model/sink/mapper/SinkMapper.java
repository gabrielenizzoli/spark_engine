package sparkengine.plan.model.sink.mapper;

import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.impl.*;

public interface SinkMapper {

    default Sink mapShowSink(ShowSink sink) throws Exception {
        return sink;
    }

    default Sink mapViewSink(ViewSink sink) throws Exception {
        return sink;
    }

    default Sink mapCounterSink(CounterSink sink) throws Exception {
        return sink;
    }

    default Sink mapBatchSink(BatchSink sink) throws Exception {
        return sink;
    }

    default Sink mapStreamSink(StreamSink sink) throws Exception {
        return sink;
    }

    default Sink mapForeachSink(ForeachSink sink) throws Exception {
        return sink;
    }

}
