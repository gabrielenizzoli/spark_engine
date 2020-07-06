package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.DataSource;

public interface DataSourceFactory<T> {

    DataSource<T> build() throws DataSourceFactoryException;

}
