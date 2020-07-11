package dataengine.pipeline.model.builder.source;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.factory.DataSourceCatalog;
import dataengine.pipeline.core.source.factory.DataSourceCatalogException;
import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.model.builder.source.factory.DataSourceFactories;
import dataengine.pipeline.model.description.source.Component;
import dataengine.pipeline.model.description.source.ComponentCatalog;
import dataengine.pipeline.model.description.source.ComponentCatalogException;
import lombok.*;

import javax.annotation.Nonnull;
import java.util.*;

@RequiredArgsConstructor
public class DataSourceCatalogImpl implements DataSourceCatalog {

    @Nonnull
    private final ComponentCatalog componentCatalog;
    @Nonnull
    private final DataSourceCache dataSourceCache = new DataSourceCache();


    private class DataSourceProxyCatalog implements DataSourceCatalog {

        @Getter
        private final Set<String> parentDataSourceNames = new HashSet<>();

        @Override
        public @Nonnull DataSource<?> lookup(String dataSourceName) throws DataSourceCatalogException {
            parentDataSourceNames.add(dataSourceName);
            return dataSourceCache.getDataSourceCacheReference(dataSourceName);
        }

    }

    @Override
    public @Nonnull DataSource<?> lookup(String dataSourceName) throws DataSourceCatalogException {

        // get from cache
        Optional<DataSourceCache.DataSourceInfo<?>> dataSourceOptional = dataSourceCache.get(dataSourceName);
        if (dataSourceOptional.isPresent()) {
            return dataSourceOptional.get().getDataSource();
        }

        try {
            Queue<String> dataSourceNames = new LinkedList<>();
            dataSourceNames.add(dataSourceName);

            while (!dataSourceNames.isEmpty()) {
                String currentDataSourceName = dataSourceNames.remove();
                DataSourceCache.DataSourceInfo<?> currentDataSourceInfo = lookupDataSourceInfo(currentDataSourceName);
                dataSourceCache.add(currentDataSourceName, currentDataSourceInfo);
                dataSourceNames.addAll(currentDataSourceInfo.getParentDataSourceNames());
            }

            return dataSourceCache.get(dataSourceName)
                    .orElseThrow(() -> new DataSourceFactoryException("no datasource with name " + dataSourceName))
                    .getDataSource();
        } catch (DataSourceFactoryException | ComponentCatalogException e) {
            throw new DataSourceCatalogException("error while building datasource with name " + dataSourceName, e);
        }
    }

    private DataSourceCache.DataSourceInfo<?> lookupDataSourceInfo(String dataSourceName) throws
            ComponentCatalogException,
            DataSourceFactoryException,
            DataSourceCatalogException {

        Optional<Component> component = componentCatalog.lookup(dataSourceName);
        if (!component.isPresent())
            throw new DataSourceCatalogException.ComponentNotFound("component for datasource " + dataSourceName + " does not exist");

        DataSourceProxyCatalog dependenciesCatalog = new DataSourceProxyCatalog();

        DataSource<?> dataSource = DataSourceFactories.factoryForComponent(component.get(), dependenciesCatalog).build();

        DataSourceCache.DataSourceInfo<?> dataSourceInfo = DataSourceCache.DataSourceInfo.builder()
                .dataSource((DataSource<Object>) dataSource)
                .component(component.get())
                .parentDataSourceNames(dependenciesCatalog.getParentDataSourceNames())
                .build();

        return dataSourceInfo;
    }

}
