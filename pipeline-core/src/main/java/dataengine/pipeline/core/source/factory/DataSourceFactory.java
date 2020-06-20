package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.DataSource;

import java.util.function.Function;

public interface DataSourceFactory extends Function<String, DataSource> {

    default DataSourceFactory withCache() {
        return dataengine.pipeline.core.source.factory.DataSourceFactoryCache.builder().dataSourceFactory(this).build();
    }

}
