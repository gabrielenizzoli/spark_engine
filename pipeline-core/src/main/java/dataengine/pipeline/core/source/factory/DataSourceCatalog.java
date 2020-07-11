package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.DataSource;

import javax.annotation.Nonnull;

@FunctionalInterface
public interface DataSourceCatalog {

    @Nonnull
    DataSource<?> lookup(String dataSourceName) throws DataSourceCatalogException;

}
