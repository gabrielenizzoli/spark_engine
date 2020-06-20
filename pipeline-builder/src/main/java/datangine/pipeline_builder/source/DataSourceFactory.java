package datangine.pipeline_builder.source;

import dataengine.pipeline.DataSource;

import java.util.function.Function;

public interface DataSourceFactory extends Function<String, DataSource> {

    default DataSourceFactory withCache() {
        return DataSourceFactoryCache.builder().dataSourceFactory(this).build();
    }

}
