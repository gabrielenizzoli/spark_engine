package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.DataSourceCache;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

@Value
@Builder
public class DataSourceFactoryCache implements dataengine.pipeline.core.source.factory.DataSourceFactory {

    @Nonnull
    dataengine.pipeline.core.source.factory.DataSourceFactory dataSourceFactory;
    @Nonnull
    @lombok.Builder.Default
    Map<String, DataSource> cachedSources = new HashMap<>();

    @Override
    public DataSource apply(String name) {
        return cachedSources.computeIfAbsent(name, n -> {
            DataSource ds = dataSourceFactory.apply(n);
            return DataSourceCache.builder().dataSource(ds).build();
        });
    }

}
