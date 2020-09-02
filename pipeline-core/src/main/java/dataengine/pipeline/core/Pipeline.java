package dataengine.pipeline.core;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.sink.composer.DataSinkComposer;
import dataengine.pipeline.core.sink.composer.DataSinkComposerException;
import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.composer.DataSourceComposer;
import dataengine.pipeline.core.source.composer.DataSourceComposerException;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder
public class Pipeline {

    @Nonnull
    DataSourceComposer dataSourceComposer;
    @Nonnull
    DataSinkComposer dataSinkComposer;

    public <T> DataSink<T> run(@Nonnull String sourceName, @Nonnull String sinkName) throws DataSourceComposerException, DataSinkComposerException {
        DataSource<T> dataSource = dataSourceComposer.lookup(sourceName);
        DataSink<T> dataSink = dataSinkComposer.lookup(sinkName);
        return dataSource.writeTo(dataSink);
    }

}
