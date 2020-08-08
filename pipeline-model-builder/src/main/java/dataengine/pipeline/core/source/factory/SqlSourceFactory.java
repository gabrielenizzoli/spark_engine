package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.impl.SparkSqlSource;
import dataengine.pipeline.model.description.source.component.SqlSource;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class SqlSourceFactory implements DataSourceFactory {

    @Nonnull
    SqlSource source;

    @Override
    public DataSource<?> build() throws DataSourceFactoryException {
        return SparkSqlSource.builder()
                .sql(source.getSql())
                .encoder(EncoderUtils.buildEncoder(source.getEncodedAs()))
                .build();
    }

}