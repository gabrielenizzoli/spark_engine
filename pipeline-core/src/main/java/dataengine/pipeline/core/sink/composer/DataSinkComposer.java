package dataengine.pipeline.core.sink.composer;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.source.DataSource;

import javax.annotation.Nonnull;

@FunctionalInterface
public interface DataSinkComposer {

    @Nonnull
    <T> DataSink<T> lookup(String dataSinkName) throws DataSinkComposerException;

}
