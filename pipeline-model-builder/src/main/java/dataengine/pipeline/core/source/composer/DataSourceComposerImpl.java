package dataengine.pipeline.core.source.composer;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.GraphException;
import dataengine.pipeline.core.source.GraphUtils;
import dataengine.pipeline.core.source.factory.DataSourceFactories;
import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.core.source.factory.EncoderUtils;
import dataengine.pipeline.core.source.impl.DataSourceReference;
import dataengine.pipeline.model.description.encoder.DataEncoder;
import dataengine.pipeline.model.description.source.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@ToString
public class DataSourceComposerImpl implements DataSourceComposer {

    @Nonnull
    ComponentCatalog componentCatalog;

    Set<String> roots = new HashSet<>();
    Map<String, DataSourceInfo<?>> dataSourceInfos = new HashMap<>();

    public static DataSourceComposerImpl ofCatalog(ComponentCatalog componentCatalog) {
        return new DataSourceComposerImpl(componentCatalog);
    }

    private class DataSourceProxyComposer implements DataSourceComposer {

        @Getter
        private final Set<String> parentDataSourceNames = new HashSet<>();

        @Override
        public @Nonnull
        DataSource<?> lookup(String dataSourceName) throws DataSourceComposerException {
            parentDataSourceNames.add(dataSourceName);
            return new DataSourceReference<>(() -> Optional.ofNullable(dataSourceInfos.get(dataSourceName)).map(DataSourceInfo::getDataSource).orElse(null));
        }

    }

    @Override
    @Nonnull
    public DataSource<?> lookup(String dataSourceName) throws DataSourceComposerException {

        // get from cache
        if (dataSourceInfos.containsKey(dataSourceName))
            return dataSourceInfos.get(dataSourceName).getDataSource();

        try {

            // stage new graph
            Map<String, DataSourceInfo<?>> stagingDataSourceInfos = stageDataSourceInfos(dataSourceName, dataSourceInfos);

            // validate
            Map<String, Set<String>> graph = new HashMap<>();
            stagingDataSourceInfos.forEach((key, value) -> graph.put(key, value.getParentDataSourceNames()));
            GraphUtils.pathsToSources(Collections.singleton(dataSourceName), graph);

            // update
            roots.add(dataSourceName);
            dataSourceInfos.clear();
            dataSourceInfos.putAll(stagingDataSourceInfos);

            return dataSourceInfos.get(dataSourceName).getDataSource();
        } catch (GraphException | DataSourceFactoryException | ComponentCatalogException e) {
            throw new DataSourceComposerException("error while building datasource with name " + dataSourceName, e);
        }
    }

    private Map<String, DataSourceInfo<?>> stageDataSourceInfos(String dataSourceName, Map<String, DataSourceInfo<?>> currentDataSourceInfos)
            throws ComponentCatalogException, DataSourceFactoryException, DataSourceComposerException {

        Map<String, DataSourceInfo<?>> stagingDataSourceInfos = new HashMap<>(currentDataSourceInfos);

        Queue<String> pendingDataSourceNames = new LinkedList<>();
        pendingDataSourceNames.add(dataSourceName);
        while (!pendingDataSourceNames.isEmpty()) {
            String newDataSourceName = pendingDataSourceNames.remove();
            if (stagingDataSourceInfos.containsKey(newDataSourceName))
                continue;
            DataSourceInfo<?> newDataSourceInfo = buildDataSourceInfo(newDataSourceName);
            stagingDataSourceInfos.put(newDataSourceName, newDataSourceInfo);
            Set<String> parents = new HashSet<>(newDataSourceInfo.getParentDataSourceNames());
            parents.removeAll(stagingDataSourceInfos.keySet());
            if (!parents.isEmpty())
                pendingDataSourceNames.addAll(parents);
        }

        return stagingDataSourceInfos;
    }

    private DataSourceInfo<?> buildDataSourceInfo(String dataSourceName) throws
            ComponentCatalogException,
            DataSourceFactoryException,
            DataSourceComposerException {

        Component component = getComponent(dataSourceName);

        DataSourceProxyComposer dataSourceProxyComposer = new DataSourceProxyComposer();
        DataSource<?> dataSource = DataSourceFactories.factoryForComponent(component, dataSourceProxyComposer).build();

        DataSourceInfo<?> dataSourceInfo = DataSourceInfoWithComponent.builder()
                .dataSource((DataSource<Object>) dataSource)
                .component(component)
                .parentDataSourceNames(dataSourceProxyComposer.getParentDataSourceNames())
                .schema(getComponentSchema(component))
                .build();

        return dataSourceInfo;
    }

    @Nonnull
    private Component getComponent(String dataSourceName) throws ComponentCatalogException, DataSourceComposerException {
        Optional<Component> componentLookup = componentCatalog.lookup(dataSourceName);
        if (!componentLookup.isPresent())
            throw new DataSourceComposerException.ComponentNotFound("component for datasource " + dataSourceName + " does not exist");
        return componentLookup.get();
    }

    @Nullable
    private StructType getComponentSchema(Component component) throws DataSourceFactoryException {
        StructType schema = null;
        if (component instanceof EncodedComponent) {
            EncodedComponent encodedComponent = (EncodedComponent) component;
            DataEncoder dataEncoder = encodedComponent.getEncodedAs();
            if (dataEncoder != null) {
                schema = EncoderUtils.buildEncoder(dataEncoder).schema();
            }
        } else if (component instanceof SchemaComponent) {
            SchemaComponent schemaComponent = (SchemaComponent) component;
            String schemaDdl = schemaComponent.getSchema();
            if (StringUtils.isNotBlank(schemaDdl)) {
                schema = StructType.fromDDL(schemaComponent.getSchema());
            }
        }
        return schema;
    }

}
