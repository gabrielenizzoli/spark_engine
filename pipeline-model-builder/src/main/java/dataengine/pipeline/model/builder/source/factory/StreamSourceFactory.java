package dataengine.pipeline.model.builder.source.factory;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.factory.DataSourceFactory;
import dataengine.pipeline.core.source.factory.DataSourceFactoryException;
import dataengine.pipeline.core.source.impl.SparkSource;
import dataengine.pipeline.model.description.source.component.StreamSource;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class StreamSourceFactory implements DataSourceFactory {

    @Nonnull
    StreamSource source;

    @Override
    public DataSource<?> build() throws DataSourceFactoryException {
        return SparkSource.builder()
                .format(source.getFormat())
                .options(source.getOptions())
                .encoder(EncoderUtils.buildEncoder(source.getAs()))
                .type(SparkSource.SourceType.STREAM)
                .build();
    }

}
