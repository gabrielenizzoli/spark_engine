package dataengine.pipeline.core.supplier.catalog;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.pipeline.core.supplier.GraphException;
import dataengine.pipeline.core.supplier.factory.DataSourceFactories;
import dataengine.pipeline.core.supplier.factory.DatasetSupplierFactoryException;
import dataengine.pipeline.core.supplier.impl.DatasetSupplierReference;
import dataengine.pipeline.core.supplier.utils.EncoderUtils;
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
public class DatasetSupplierCatalogImpl implements DatasetSupplierCatalog {

    @Nonnull
    ComponentCatalog componentCatalog;

    Set<String> roots = new HashSet<>();
    Map<String, DataSourceInfo<?>> dataSourceInfos = new HashMap<>();

    public static DatasetSupplierCatalogImpl ofCatalog(ComponentCatalog componentCatalog) {
        return new DatasetSupplierCatalogImpl(componentCatalog);
    }

    @Value
    private class DataSourceProxyComposer implements DatasetSupplierCatalog {

        @Nonnull
        Set<String> parentDataSourceNames;

        @Nonnull
        @Override
        public DatasetSupplier<?> lookup(String dataSourceName) throws DatasetSupplierCatalogException {
            if (!parentDataSourceNames.contains(dataSourceName))
                throw new DatasetSupplierCatalogException("this datasource composer does not provide a datasource with name " + dataSourceName + ", available datasources are " + parentDataSourceNames);
            return new DatasetSupplierReference<>(() -> Optional.ofNullable(dataSourceInfos.get(dataSourceName)).map(DataSourceInfo::getDatasetSupplier).orElse(null));
        }

    }

    @Override
    @Nonnull
    public <T> DatasetSupplier<T> lookup(String dataSourceName) throws DatasetSupplierCatalogException {

        // get from cache
        if (dataSourceInfos.containsKey(dataSourceName))
            return (DatasetSupplier<T>)dataSourceInfos.get(dataSourceName).getDatasetSupplier();

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

            return (DatasetSupplier<T>)dataSourceInfos.get(dataSourceName).<T>getDatasetSupplier();
        } catch (GraphException | DatasetSupplierFactoryException | ComponentCatalogException e) {
            throw new DatasetSupplierCatalogException("error while building datasource with name " + dataSourceName, e);
        }
    }

    private Map<String, DataSourceInfo<?>> stageDataSourceInfos(String dataSourceName, Map<String, DataSourceInfo<?>> currentDataSourceInfos)
            throws ComponentCatalogException, DatasetSupplierFactoryException, DatasetSupplierCatalogException {

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
            DatasetSupplierFactoryException,
            DatasetSupplierCatalogException {

        Component component = getComponent(dataSourceName);
        StructType schema = getComponentSchema(component);
        Set<String> dependencies = getComponentDependencies(component);
        DatasetSupplier<?> datasetSupplier = DataSourceFactories.factoryForComponent(component, new DataSourceProxyComposer(dependencies)).build();

        return DataSourceInfo.builder()
                .datasetSupplier((DatasetSupplier<Object>) datasetSupplier)
                .parentDataSourceNames(dependencies)
                .component(component)
                .schema(schema)
                .build();
    }

    @Nonnull
    private Component getComponent(String dataSourceName) throws ComponentCatalogException, DatasetSupplierCatalogException {
        Optional<Component> componentLookup = componentCatalog.lookup(dataSourceName);
        if (!componentLookup.isPresent())
            throw new DatasetSupplierCatalogException.ComponentNotFound("component for datasource " + dataSourceName + " does not exist");
        return componentLookup.get();
    }

    @Nullable
    private StructType getComponentSchema(Component component) throws DatasetSupplierFactoryException {
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
