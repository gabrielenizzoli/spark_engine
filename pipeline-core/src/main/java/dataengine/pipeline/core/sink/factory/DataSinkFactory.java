package dataengine.pipeline.core.sink.factory;

import dataengine.pipeline.core.sink.DataSink;

import javax.annotation.Nonnull;

@FunctionalInterface
public interface DataSinkFactory<T> {

    @Nonnull
    DataSink<T> build() throws DataSinkFactoryException;

}
