package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.DataSource;

@FunctionalInterface
public interface DataSourceCatalog {

    DataSource<?> lookup(String dataSourceName) throws DataSourceCatalogException;

}
