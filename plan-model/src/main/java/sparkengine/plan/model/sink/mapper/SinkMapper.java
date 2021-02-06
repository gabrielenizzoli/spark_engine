package sparkengine.plan.model.sink.mapper;

import sparkengine.plan.model.sink.Sink;
import sparkengine.plan.model.sink.impl.ShowSink;

public interface SinkMapper {

    default Sink mapShowSink(ShowSink showSink) throws Exception {
        return showSink;
    }

}
