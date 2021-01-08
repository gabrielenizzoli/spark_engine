package dataengine.pipeline.model.sink;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import dataengine.pipeline.model.sink.impl.*;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = BatchSink.class, name = "batch"),
        @JsonSubTypes.Type(value = StreamSink.class, name = "stream"),
        @JsonSubTypes.Type(value = ForeachBatchSink.class, name = "foreachBatch"),
        @JsonSubTypes.Type(value = ShowSink.class, name = "show"),
        @JsonSubTypes.Type(value = CollectSink.class, name = "collect")
})
public interface Sink {

}
