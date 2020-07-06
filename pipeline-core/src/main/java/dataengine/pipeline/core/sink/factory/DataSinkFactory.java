package dataengine.pipeline.core.sink.factory;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.source.DataSource;

import javax.annotation.Nonnull;
import java.util.function.Function;
import java.util.function.Supplier;

public interface DataSinkFactory<T> extends Supplier<DataSink<T>> {

}
