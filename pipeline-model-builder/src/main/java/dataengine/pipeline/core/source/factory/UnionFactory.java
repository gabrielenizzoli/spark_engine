package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.composer.DataSourceComposer;
import dataengine.pipeline.core.source.composer.DataSourceComposerException;
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
    DataSourceComposer dataSourceComposer;

    @Override
    public DataSource<?> build() throws DataSourceFactoryException {
        Validate.multiInput(2, null).accept(union);

        List<DataSource> dataSources = new ArrayList<>(union.getUsing().size());
        for (String name : union.getUsing()) {
            Validate.notBlank("source name").accept(name);
            try {
                dataSources.add(dataSourceComposer.lookup(name));
            } catch (DataSourceComposerException e) {
                throw new DataSourceFactoryException("can't locate datasource with name " + name, e);
            }
        }
        return dataSources.get(0).union(dataSources.subList(1, dataSources.size()));
    }

}
