package sparkengine.plan.model.sink.visitor;

import sparkengine.plan.model.common.Location;
import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.impl.*;

public interface SinkVisitor {

    default void visitShowSink(Location location, ShowSink sink) throws Exception {
    }

    default void visitViewSink(Location location, ViewSink sink) throws Exception {
    }

    default void visitCounterSink(Location location, CounterSink sink) throws Exception {
    }

    default void visitBatchSink(Location location, BatchSink sink) throws Exception {
    }

    default void visitStreamSink(Location location, StreamSink sink) throws Exception {
    }

    default void visitForeachSink(Location location, ForeachSink sink) throws Exception {
    }

    default void visitReferenceSink(Location location, ReferenceSink sink) throws Exception {
    }

}
