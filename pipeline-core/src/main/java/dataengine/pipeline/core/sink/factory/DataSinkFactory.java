package dataengine.pipeline.core.sink.factory;

import dataengine.pipeline.core.sink.DataSink;

@FunctionalInterface
public interface DataSinkFactory<T> {

    DataSink<T> build() throws DataSinkFactoryException;

}
