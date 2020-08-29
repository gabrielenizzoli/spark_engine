package dataengine.pipeline.core.source.composer;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.GraphException;
import dataengine.pipeline.core.source.factory.DataSourceFactories;
import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.core.source.impl.DataSourceReference;
import dataengine.pipeline.core.source.utils.EncoderUtils;
import dataengine.pipeline.core.utils.GraphUtils;
import dataengine.pipeline.model.description.encoder.DataEncoder;
import dataengine.pipeline.model.description.source.*;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.Value;
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

    @Value
    private class DataSourceProxyComposer implements DataSourceComposer {

        @Nonnull
        Set<String> parentDataSourceNames;

        @Nonnull
        @Override
        public DataSource<?> lookup(String dataSourceName) throws DataSourceComposerException {
            if (!parentDataSourceNames.contains(dataSourceName))
                throw new DataSourceComposerException("this datasource composer does not provide a datasource with name " + dataSourceName + ", available datasources are " + parentDataSourceNames);
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
        StructType schema = getComponentSchema(component);
        Set<String> dependencies = getComponentDependencies(component);
        DataSource<?> dataSource = DataSourceFactories.factoryForComponent(component, new DataSourceProxyComposer(dependencies)).build();

        return DataSourceInfo.builder()
                .dataSource((DataSource<Object>) dataSource)
                .parentDataSourceNames(dependencies)
                .component(component)
                .schema(schema)
                .build();
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
                // TODO may throw exception
                schema = StructType.fromDDL(schemaDdl);
            }
        }
        return schema;
    }

    @Nonnull
    private Set<String> getComponentDependencies(Component component) {
        if (component instanceof TransformationComponentWithMultipleInputs) {
            return new HashSet<>(((TransformationComponentWithMultipleInputs) component).getUsing());
        }
        if (component instanceof TransformationComponentWithSingleInput) {
            return Collections.singleton(((TransformationComponentWithSingleInput) component).getUsing());
        }
        return Collections.emptySet();
    }

}
