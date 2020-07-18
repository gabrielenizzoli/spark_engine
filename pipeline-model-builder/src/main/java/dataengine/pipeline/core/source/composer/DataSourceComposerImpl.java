package dataengine.pipeline.core.source.composer;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.cache.DataSourceCache;
import dataengine.pipeline.core.source.cache.DataSourceCacheException;
import dataengine.pipeline.core.source.cache.DataSourceCacheReference;
import dataengine.pipeline.core.source.cache.DataSourceInfo;
import dataengine.pipeline.core.source.cache.impl.DataSourceCacheImpl;
import dataengine.pipeline.core.source.factory.DataSourceFactories;
import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.core.source.factory.EncoderUtils;
import dataengine.pipeline.model.description.encoder.DataEncoder;
import dataengine.pipeline.model.description.source.*;
import lombok.*;
import lombok.experimental.FieldDefaults;
import lombok.experimental.SuperBuilder;
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
    @Getter
    DataSourceCache dataSourceCache = new DataSourceCacheImpl();

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
            return new DataSourceCacheReference<>(dataSourceName, dataSourceCache);
        }

    }

    @Getter
    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    @EqualsAndHashCode(callSuper = true)
    @ToString(callSuper = true)
    @SuperBuilder
    public static class DataSourceInfoWithComponent<T> extends DataSourceInfo<T> {

        @Nonnull
        Component component;

    }

    interface DataSourceProvider {
        DataSourceInfo<?> dataSourceInfo(String dataSourceName) throws
                ComponentCatalogException,
                DataSourceFactoryException,
                DataSourceComposerException;
    }

    @Override
    @Nonnull
    public DataSource<?> lookup(String dataSourceName) throws DataSourceComposerException {

        // get from cache
        Optional<DataSourceInfo<?>> dataSourceOptional = dataSourceCache.get(dataSourceName);
        if (dataSourceOptional.isPresent()) {
            return dataSourceOptional.get().getDataSource();
        }

        try {
            new DiscoveredDataSources(dataSourceName)
                    .discover(this::buildDataSourceInfo)
                    .fillCache(dataSourceCache);

            return dataSourceCache.get(dataSourceName)
                    .orElseThrow(() -> new DataSourceFactoryException("no datasource with name " + dataSourceName))
                    .getDataSource();
        } catch (DataSourceCacheException | DataSourceFactoryException | ComponentCatalogException e) {
            throw new DataSourceComposerException("error while building datasource with name " + dataSourceName, e);
        }
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
    private Component getComponent(String dataSourceName) throws ComponentCatalogException, DataSourceComposerException.ComponentNotFound {
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
