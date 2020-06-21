package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.DataSource;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

@Value
@Builder
public class DataSourceFactoryLookupStore implements DataSourceFactory {

    @AllArgsConstructor
    @Getter
    public enum StoreMode {
        NONE(false, false),
        DATASOURCE(true, false),
        DATASOURCE_AND_DATASET(true,true);

        private final boolean storeDatasource;
        private final boolean storeDataset;

    }

    @Nonnull
    DataSourceFactory dataSourceFactory;
    @Nonnull
    @lombok.Builder.Default
    StoreMode storeMode = StoreMode.DATASOURCE_AND_DATASET;
    @Nonnull
    @lombok.Builder.Default
    Map<String, DataSource> cachedSources = new HashMap<>();

    @Override
    public DataSource apply(String name) {
        if (!storeMode.isStoreDatasource())
            return dataSourceFactory.apply(name);
        return cachedSources.computeIfAbsent(name, n -> {
            DataSource ds = dataSourceFactory.apply(n);
            return storeMode.isStoreDataset() ? ds.withDatasetStore() : ds;
        });
    }

}
