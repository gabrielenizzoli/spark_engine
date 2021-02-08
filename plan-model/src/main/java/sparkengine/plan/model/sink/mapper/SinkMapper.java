package sparkengine.plan.model.sink.mapper;

import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.impl.*;

public interface SinkMapper {

    default Sink mapShowSink(Location location, ShowSink sink) throws Exception {
        return sink;
    }

    default Sink mapViewSink(Location location, ViewSink sink) throws Exception {
        return sink;
    }

    default Sink mapCounterSink(Location location, CounterSink sink) throws Exception {
        return sink;
    }

    default Sink mapBatchSink(Location location, BatchSink sink) throws Exception {
        return sink;
    }

    default Sink mapStreamSink(Location location, StreamSink sink) throws Exception {
        return sink;
    }

    default Sink mapForeachSink(Location location, ForeachSink sink) throws Exception {
        return sink;
    }

    default Sink mapReferenceSink(Location location, ReferenceSink sink) throws Exception {
        return sink;
    }

}
