package dataengine.pipeline.model.builder.source.factory;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.factory.DataSourceCatalog;
import dataengine.pipeline.core.source.factory.DataSourceCatalogException;
import dataengine.pipeline.core.source.factory.DataSourceFactory;
import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.model.description.source.component.Encode;
import lombok.Value;
import org.apache.spark.sql.Encoder;

import javax.annotation.Nonnull;

@Value
public class EncodeFactory implements DataSourceFactory {

    @Nonnull
    Encode encode;
    @Nonnull
    DataSourceCatalog dataSourceCatalog;

    @Override
    public DataSource<?> build() throws DataSourceFactoryException {
        Validate.singleInput().accept(encode);
        Encoder<?> encoder = EncoderUtils.buildEncoder(encode.getAs());
        try {
            return dataSourceCatalog
                    .lookup(encode.getUsing())
                    .encodeAs(encoder);
        } catch (DataSourceCatalogException e) {
            throw new DataSourceFactoryException("can't locate datasource with name " + encode.getUsing(), e);
        }
    }

}
