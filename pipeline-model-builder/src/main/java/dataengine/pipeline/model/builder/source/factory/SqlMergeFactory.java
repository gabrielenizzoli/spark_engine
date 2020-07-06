package dataengine.pipeline.model.builder.source.factory;

import dataengine.pipeline.core.source.factory.DataSourceCatalogException;
import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.DataSourceMerge;
import dataengine.pipeline.core.source.factory.DataSourceCatalog;
import dataengine.pipeline.core.source.factory.DataSourceFactory;
import dataengine.pipeline.model.description.source.component.SqlMerge;
import dataengine.spark.transformation.SqlTransformations;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class SqlMergeFactory implements DataSourceFactory {

    @Nonnull
    SqlMerge sqlMerge;
    @Nonnull
    DataSourceCatalog dataSourceCatalog;

    @Override
    public DataSource<?> build() throws DataSourceFactoryException {
        Validate.multiInput(2, 5).accept(sqlMerge);
        switch (sqlMerge.getUsing().size()) {
            case 2:
                return DataSourceMerge.mergeAll(
                        lookupDataSource(sqlMerge.getUsing().get(0)),
                        lookupDataSource(sqlMerge.getUsing().get(1)),
                        SqlTransformations.sqlMerge(
                                sqlMerge.getUsing().get(0),
                                sqlMerge.getUsing().get(1),
                                sqlMerge.getSql())
                );
            case 3:
                return DataSourceMerge.mergeAll(
                        lookupDataSource(sqlMerge.getUsing().get(0)),
                        lookupDataSource(sqlMerge.getUsing().get(1)),
                        lookupDataSource(sqlMerge.getUsing().get(2)),
                        SqlTransformations.sqlMerge(
                                sqlMerge.getUsing().get(0),
                                sqlMerge.getUsing().get(1),
                                sqlMerge.getUsing().get(2),
                                sqlMerge.getSql())
                );
            case 4:
                return DataSourceMerge.mergeAll(
                        lookupDataSource(sqlMerge.getUsing().get(0)),
                        lookupDataSource(sqlMerge.getUsing().get(1)),
                        lookupDataSource(sqlMerge.getUsing().get(2)),
                        lookupDataSource(sqlMerge.getUsing().get(3)),
                        SqlTransformations.sqlMerge(
                                sqlMerge.getUsing().get(0),
                                sqlMerge.getUsing().get(1),
                                sqlMerge.getUsing().get(2),
                                sqlMerge.getUsing().get(3),
                                sqlMerge.getSql())
                );
            case 5:
                return DataSourceMerge.mergeAll(
                        lookupDataSource(sqlMerge.getUsing().get(0)),
                        lookupDataSource(sqlMerge.getUsing().get(1)),
                        lookupDataSource(sqlMerge.getUsing().get(2)),
                        lookupDataSource(sqlMerge.getUsing().get(3)),
                        lookupDataSource(sqlMerge.getUsing().get(4)),
                        SqlTransformations.sqlMerge(
                                sqlMerge.getUsing().get(0),
                                sqlMerge.getUsing().get(1),
                                sqlMerge.getUsing().get(2),
                                sqlMerge.getUsing().get(3),
                                sqlMerge.getUsing().get(4),
                                sqlMerge.getSql())
                );
        }
        throw new DataSourceFactoryException(sqlMerge + " step not valid");
    }

    private DataSource<?> lookupDataSource(String name) throws DataSourceFactoryException {
        try {
            return dataSourceCatalog.lookup(name);
        } catch (DataSourceCatalogException e) {
            throw new DataSourceFactoryException("can't locate datasource with name " + name, e);
        }
    }

}
