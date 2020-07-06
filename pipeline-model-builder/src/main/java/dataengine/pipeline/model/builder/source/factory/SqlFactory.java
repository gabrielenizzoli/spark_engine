package dataengine.pipeline.model.builder.source.factory;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.factory.DataSourceCatalog;
import dataengine.pipeline.core.source.factory.DataSourceCatalogException;
import dataengine.pipeline.core.source.factory.DataSourceFactory;
import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.model.description.source.component.Sql;
import dataengine.spark.transformation.SqlTransformations;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class SqlFactory implements DataSourceFactory {

    @Nonnull
    Sql sql;
    @Nonnull
    DataSourceCatalog dataSourceCatalog;

    @Override
    public DataSource<?> build() throws DataSourceFactoryException {
        Validate.singleInput().accept(sql);
        try {
            return dataSourceCatalog
                    .lookup(sql.getUsing())
                    .transform(SqlTransformations.sql(sql.getUsing(), sql.getSql()));
        } catch (DataSourceCatalogException e) {
            throw new DataSourceFactoryException("can't locate datasource with name " + sql.getUsing(), e);
        }
    }

}
