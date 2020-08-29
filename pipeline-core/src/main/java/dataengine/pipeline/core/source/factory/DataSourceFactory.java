package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.DataSource;

public interface DataSourceFactory<T> {

    @Nonnull
    DataSource<T> build() throws DataSourceFactoryException;

}
