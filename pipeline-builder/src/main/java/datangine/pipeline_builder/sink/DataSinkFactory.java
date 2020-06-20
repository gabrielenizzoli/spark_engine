package datangine.pipeline_builder.sink;

import dataengine.model.pipeline.sink.Sink;
import dataengine.pipeline.DataSink;

import java.util.function.Function;

public interface DataSinkFactory extends Function<Sink, DataSink> {
}
