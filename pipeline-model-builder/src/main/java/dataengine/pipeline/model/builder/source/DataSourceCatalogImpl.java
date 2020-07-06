package dataengine.pipeline.model.builder.source;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.factory.DataSourceCatalog;
import dataengine.pipeline.core.source.factory.DataSourceCatalogException;
import dataengine.pipeline.core.source.factory.DataSourceFactory;
import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.model.builder.source.factory.*;
import dataengine.pipeline.model.description.source.*;
import lombok.AllArgsConstructor;

import javax.annotation.Nonnull;

@AllArgsConstructor
public class DataSourceCatalogImpl implements DataSourceCatalog {

    @Nonnull
    ComponentCatalog componentCatalog;

    public static DataSourceCatalogImpl withComponentCatalog(ComponentCatalog componentCatalog) {
        return new DataSourceCatalogImpl(componentCatalog);
    }

    @Override
    public DataSource<?> lookup(String dataSourceName) throws DataSourceCatalogException {
        try {
            Component component = componentCatalog.lookup(dataSourceName);
            DataSourceFactory<?> dataSourceFactory = DataSourceFactories.factoryForComponent(component, this);
            return dataSourceFactory.build();
        } catch (DataSourceFactoryException | ComponentCatalogException e) {
            throw  new DataSourceCatalogException("error while building datasource with name " + dataSourceName, e);
        }
    }

}
