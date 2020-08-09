package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.composer.DataSourceComposerException;
import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.DataSourceMerge;
import dataengine.pipeline.core.source.composer.DataSourceComposer;
import dataengine.pipeline.core.source.utils.EncoderUtils;
import dataengine.pipeline.core.source.utils.UdfUtils;
import dataengine.pipeline.model.description.source.component.Sql;
import dataengine.spark.sql.udf.UdfCollection;
import dataengine.spark.transformation.SqlTransformations;
import dataengine.spark.transformation.Transformations;
import lombok.Value;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

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

        Validate.multiInput(1, null).accept(sql);

        UdfCollection udfCollection = UdfUtils.buildUdfCollection(sql.getUdfs());

        return DataSourceMerge.mergeAll(
                getDataSources(),
                SqlTransformations.sqlMerge(sql.getUsing(), sql.getSql(), udfCollection)
        );
    }

    @Nonnull
    private List<DataSource<Row>> getDataSources() throws DataSourceFactoryException {
        List<DataSource<Row>> dataSources = new ArrayList<>(sql.getUsing().size());
        for (int i = 0; i < sql.getUsing().size(); i++)
            dataSources.add(lookupDataSource(sql.getUsing().get(i)).transform(Transformations.encodeAsRow()));
        return dataSources;
    }

    private DataSource<?> lookupDataSource(String name) throws DataSourceFactoryException {
        try {
            return dataSourceComposer.lookup(name);
        } catch (DataSourceComposerException e) {
            throw new DataSourceFactoryException("can't locate datasource with name " + name, e);
        }
    }

}
