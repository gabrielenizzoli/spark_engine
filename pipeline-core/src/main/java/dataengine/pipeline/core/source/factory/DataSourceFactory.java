package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.DataSource;

import javax.annotation.Nonnull;

@FunctionalInterface
public interface DataSourceFactory<T> {

    @Nonnull
    DataSource<T> build() throws DataSourceFactoryException;

}
