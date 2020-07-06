package dataengine.pipeline.model.builder.source.factory;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.factory.DataSourceCatalog;
import dataengine.pipeline.core.source.factory.DataSourceCatalogException;
import dataengine.pipeline.core.source.factory.DataSourceFactory;
import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.model.description.source.component.Union;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

@Value
public class UnionFactory implements DataSourceFactory {

    @Nonnull
    Union union;
    @Nonnull
    DataSourceCatalog dataSourceCatalog;

    @Override
    public DataSource<?> build() throws DataSourceFactoryException {
        Validate.multiInput(2, null).accept(union);

        List<DataSource> dataSources = new ArrayList<>(union.getUsing().size());
        for (String name : union.getUsing()) {
            Validate.notBlank("source name").accept(name);
            try {
                dataSources.add(dataSourceCatalog.lookup(name));
            } catch (DataSourceCatalogException e) {
                throw new DataSourceFactoryException("can't locate datasource with name " + name, e);
            }
        }
        return dataSources.get(0).union(dataSources.subList(1, dataSources.size()));
    }

}
