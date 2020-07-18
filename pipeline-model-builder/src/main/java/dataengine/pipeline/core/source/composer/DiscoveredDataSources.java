package dataengine.pipeline.core.source.composer;

import dataengine.pipeline.core.source.cache.DataSourceCache;
import dataengine.pipeline.core.source.cache.DataSourceCacheException;
import dataengine.pipeline.core.source.cache.DataSourceInfo;
import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.model.description.source.ComponentCatalogException;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;

import javax.annotation.Nonnull;
import java.util.*;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
class DiscoveredDataSources {
    Queue<String> pendingDataSourceNames = new LinkedList<>();
    Map<String, DataSourceInfo<?>> dataSourceInfos = new LinkedHashMap<>();

    public DiscoveredDataSources(@Nonnull String startingDataSourceName) {
        pendingDataSourceNames.add(Objects.requireNonNull(startingDataSourceName, "starting datasource name is null"));
    }

    private void add(@Nonnull String dataSourceName, @Nonnull DataSourceInfo<?> dataSourceInfo) {
        if (dataSourceInfos.containsKey(Objects.requireNonNull(dataSourceName, "new datasource name is null")))
            return;
        dataSourceInfos.put(dataSourceName, Objects.requireNonNull(dataSourceInfo, "new datasource"));
        Set<String> newDataSourceNames = new HashSet<>(dataSourceInfo.getParentDataSourceNames());
        newDataSourceNames.removeAll(dataSourceInfos.keySet());
        pendingDataSourceNames.addAll(newDataSourceNames);
    }

    public DiscoveredDataSources discover(DataSourceComposerImpl.DataSourceProvider dataSourceProvider) throws
            ComponentCatalogException,
            DataSourceFactoryException,
            DataSourceComposerException {
        while (!pendingDataSourceNames.isEmpty()) {
            String currentDataSourceName = pendingDataSourceNames.remove();
            DataSourceInfo<?> currentDataSourceInfo = dataSourceProvider.dataSourceInfo(currentDataSourceName);
            add(currentDataSourceName, currentDataSourceInfo);
        }
        return this;
    }

    public void fillCache(DataSourceCache dataSourceCache) throws DataSourceCacheException {
        List<String> dataSourceNames = new ArrayList<>(dataSourceInfos.keySet());
        Collections.reverse(dataSourceNames);

        for (String dataSourceName : dataSourceNames) {
            dataSourceCache.add(dataSourceName, dataSourceInfos.get(dataSourceName));
        }
    }

}
