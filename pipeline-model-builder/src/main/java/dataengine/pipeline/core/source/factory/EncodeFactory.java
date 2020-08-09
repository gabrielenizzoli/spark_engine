package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.composer.DataSourceComposer;
import dataengine.pipeline.core.source.composer.DataSourceComposerException;
import dataengine.pipeline.core.source.utils.EncoderUtils;
import dataengine.pipeline.model.description.source.component.Encode;
import lombok.Value;
import org.apache.spark.sql.Encoder;

import javax.annotation.Nonnull;

@Value
public class EncodeFactory implements DataSourceFactory {

    @Nonnull
    Encode encode;
    @Nonnull
    DataSourceComposer dataSourceComposer;

    @Override
    public DataSource<?> build() throws DataSourceFactoryException {
        Validate.singleInput().accept(encode);
        Encoder<?> encoder = EncoderUtils.buildEncoder(encode.getEncodedAs());
        try {
            return dataSourceComposer
                    .lookup(encode.getUsing())
                    .encodeAs(encoder);
        } catch (DataSourceComposerException e) {
            throw new DataSourceFactoryException("can't locate datasource with name " + encode.getUsing(), e);
        }
    }

}
