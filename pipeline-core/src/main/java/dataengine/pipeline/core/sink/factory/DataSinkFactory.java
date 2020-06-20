package dataengine.pipeline.core.sink.factory;

import dataengine.pipeline.core.sink.DataSink;

import java.util.function.Function;

public interface DataSinkFactory extends Function<String, DataSink> {
}
