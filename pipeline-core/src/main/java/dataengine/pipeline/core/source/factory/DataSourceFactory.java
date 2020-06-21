package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.DataSource;

import java.util.function.Function;

public interface DataSourceFactory extends Function<String, DataSource<?>> {

    default DataSourceFactory withLookupCache() {
        if (this instanceof DataSourceFactoryLookupStore)
            return this;
        return DataSourceFactoryLookupStore.builder().dataSourceFactory(this).build();
    }

}
