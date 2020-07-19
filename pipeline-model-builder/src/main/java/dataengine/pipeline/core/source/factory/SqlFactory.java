package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.composer.DataSourceComposerException;
import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.DataSourceMerge;
import dataengine.pipeline.core.source.composer.DataSourceComposer;
import dataengine.pipeline.model.description.source.component.Sql;
import dataengine.spark.transformation.SqlTransformations;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class SqlFactory implements DataSourceFactory {

    @Nonnull
    Sql sql;
    @Nonnull
    DataSourceComposer dataSourceComposer;

    @Override
    public DataSource<?> build() throws DataSourceFactoryException {
        DataSource<?> dataSource = getDataSource();
        if (sql.getEncodedAs() == null)
            return dataSource;
        return dataSource.encodeAs(EncoderUtils.buildEncoder(sql.getEncodedAs()));
    }

    private DataSource<?> getDataSource() throws DataSourceFactoryException {
        Validate.multiInput(1, 5).accept(sql);
        switch (sql.getUsing().size()) {
            case 1:
                return lookupDataSource(sql.getUsing().get(0))
                        .transform(SqlTransformations.sql(sql.getUsing().get(0), sql.getSql()));
            case 2:
                return DataSourceMerge.mergeAll(
                        lookupDataSource(sql.getUsing().get(0)),
                        lookupDataSource(sql.getUsing().get(1)),
                        SqlTransformations.sqlMerge(
                                sql.getUsing().get(0),
                                sql.getUsing().get(1),
                                sql.getSql())
                );
            case 3:
                return DataSourceMerge.mergeAll(
                        lookupDataSource(sql.getUsing().get(0)),
                        lookupDataSource(sql.getUsing().get(1)),
                        lookupDataSource(sql.getUsing().get(2)),
                        SqlTransformations.sqlMerge(
                                sql.getUsing().get(0),
                                sql.getUsing().get(1),
                                sql.getUsing().get(2),
                                sql.getSql())
                );
            case 4:
                return DataSourceMerge.mergeAll(
                        lookupDataSource(sql.getUsing().get(0)),
                        lookupDataSource(sql.getUsing().get(1)),
                        lookupDataSource(sql.getUsing().get(2)),
                        lookupDataSource(sql.getUsing().get(3)),
                        SqlTransformations.sqlMerge(
                                sql.getUsing().get(0),
                                sql.getUsing().get(1),
                                sql.getUsing().get(2),
                                sql.getUsing().get(3),
                                sql.getSql())
                );
            case 5:
                return DataSourceMerge.mergeAll(
                        lookupDataSource(sql.getUsing().get(0)),
                        lookupDataSource(sql.getUsing().get(1)),
                        lookupDataSource(sql.getUsing().get(2)),
                        lookupDataSource(sql.getUsing().get(3)),
                        lookupDataSource(sql.getUsing().get(4)),
                        SqlTransformations.sqlMerge(
                                sql.getUsing().get(0),
                                sql.getUsing().get(1),
                                sql.getUsing().get(2),
                                sql.getUsing().get(3),
                                sql.getUsing().get(4),
                                sql.getSql())
                );
        }
        throw new DataSourceFactoryException(sql + " step not valid");
    }

    private DataSource<?> lookupDataSource(String name) throws DataSourceFactoryException {
        try {
            return dataSourceComposer.lookup(name);
        } catch (DataSourceComposerException e) {
            throw new DataSourceFactoryException("can't locate datasource with name " + name, e);
        }
    }

}
