package dataengine.pipeline.core.source.composer;

import dataengine.pipeline.core.source.DataSource;

import javax.annotation.Nonnull;

@FunctionalInterface
public interface DataSourceComposer {

    @Nonnull
    <T> DataSource<T> lookup(String dataSourceName) throws DataSourceComposerException;

}
