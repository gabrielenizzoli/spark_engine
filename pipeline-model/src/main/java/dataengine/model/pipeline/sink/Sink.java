package dataengine.model.pipeline.sink;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = SparkBatchSink.class, name = "batch"),
        @JsonSubTypes.Type(value = SparkStreamSink.class, name = "stream"),
        @JsonSubTypes.Type(value = SparkStreamForeachBatchSink.class, name = "streamForeachBatch")
})
public interface Sink {

}
