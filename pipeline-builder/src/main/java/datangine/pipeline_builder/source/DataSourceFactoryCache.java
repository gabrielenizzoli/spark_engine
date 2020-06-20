package datangine.pipeline_builder.source;

import dataengine.pipeline.DataSource;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

@Value
@Builder
public class DataSourceFactoryCache implements DataSourceFactory {

    @Nonnull
    DataSourceFactory dataSourceFactory;
    @Nonnull
    @Builder.Default
    Map<String, DataSource> cachedSources = new HashMap<>();

    @Override
    public DataSource apply(String name) {
        return cachedSources.computeIfAbsent(name, dataSourceFactory::apply);
    }
}
