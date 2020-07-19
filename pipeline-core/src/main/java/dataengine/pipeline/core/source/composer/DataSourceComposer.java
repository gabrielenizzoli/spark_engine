package dataengine.pipeline.core.source.composer;

import dataengine.pipeline.core.source.DataSource;

import javax.annotation.Nonnull;

@FunctionalInterface
public interface DataSourceComposer {

    @Nonnull
    DataSource<?> lookup(String dataSourceName) throws DataSourceComposerException;

}
