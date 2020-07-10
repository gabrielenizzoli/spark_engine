package dataengine.pipeline.model.builder.source;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.DataSourceError;
import dataengine.pipeline.core.source.factory.DataSourceCatalog;
import dataengine.pipeline.core.source.factory.DataSourceCatalogException;
import dataengine.pipeline.core.source.factory.DataSourceFactory;
import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.model.builder.source.factory.DataSourceFactories;
import dataengine.pipeline.model.description.source.Component;
import dataengine.pipeline.model.description.source.ComponentCatalog;
import dataengine.pipeline.model.description.source.ComponentCatalogException;
import lombok.*;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;
import java.util.*;

@RequiredArgsConstructor
public class DataSourceCatalogImpl implements DataSourceCatalog {

    @Nonnull
    private final ComponentCatalog componentCatalog;
    @Nonnull
    private final Map<String, DataSourceInfo<?>> dataSources = new HashMap<>();

    @Value
    @Builder
    public static class DataSourceInfo<T> {

        @Nonnull
        DataSource<T> dataSource;
        @Nonnull
        Component component;
        @Nonnull
        @Singular
        Set<String> parentDataSourceNames;

    }

    @Value
    private class DataSourceProxy<T> implements DataSource<T> {

        @Nonnull
        String dataSourceName;

        @Override
        @SuppressWarnings("unchecked")
        public Dataset<T> get() {
            if (!dataSources.containsKey(dataSourceName)) {
                throw new DataSourceError("datasource " + dataSourceName + " not found");
            }
            return (Dataset<T>) dataSources.get(dataSourceName).getDataSource().get();
        }

    }

    private class DataSourceProxyCatalog implements DataSourceCatalog {

        @Getter
        private final Set<String> parentDataSourceNames = new HashSet<>();

        @Override
        public DataSource<?> lookup(String dataSourceName) throws DataSourceCatalogException {
            parentDataSourceNames.add(dataSourceName);
            return new DataSourceProxy<>(dataSourceName);
        }

    }

    @Override
    public DataSource<?> lookup(String dataSourceName) throws DataSourceCatalogException {

        // get from cache
        if (dataSources.containsKey(dataSourceName)) {
            return dataSources.get(dataSourceName).getDataSource();
        }

        try {
            Queue<String> dataSourceNames = new LinkedList<>();
            dataSourceNames.add(dataSourceName);

            while (!dataSourceNames.isEmpty()) {
                String currentDataSourceName = dataSourceNames.remove();
                DataSourceInfo<?> currentDataSourceInfo = getDataSourceInfo(currentDataSourceName);
                dataSources.put(currentDataSourceName, currentDataSourceInfo);
                avoidRecursion(currentDataSourceName);
                dataSourceNames.addAll(currentDataSourceInfo.getParentDataSourceNames());
            }

            return dataSources.get(dataSourceName).getDataSource();
        } catch (DataSourceFactoryException | ComponentCatalogException e) {
            throw new DataSourceCatalogException("error while building datasource with name " + dataSourceName, e);
        }
    }

    private void avoidRecursion(String parentName) {

        /*
        TODO
        List<List<String>> lists = new LinkedList<>();
        lists.add(Collections.singletonList(parentName));

        while (!lists.isEmpty()) {

            dataSources.forEach((childName, info) -> {
            });

        }
        */

    }

    private DataSourceInfo<?> getDataSourceInfo(String dataSourceName) throws ComponentCatalogException, DataSourceFactoryException {
        Component component = componentCatalog.lookup(dataSourceName);

        DataSourceProxyCatalog dependenciesCatalog = new DataSourceProxyCatalog();

        DataSource<?> dataSource = DataSourceFactories.factoryForComponent(component, dependenciesCatalog).build();

        DataSourceInfo<?> dataSourceInfo = DataSourceInfo.builder()
                .dataSource((DataSource<Object>) dataSource)
                .component(component)
                .parentDataSourceNames(dependenciesCatalog.getParentDataSourceNames())
                .build();

        return dataSourceInfo;
    }

}
