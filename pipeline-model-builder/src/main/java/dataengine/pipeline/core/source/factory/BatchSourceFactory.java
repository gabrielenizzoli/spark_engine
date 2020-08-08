package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.impl.SparkSource;
import dataengine.pipeline.model.description.source.component.BatchSource;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class BatchSourceFactory implements DataSourceFactory {

    @Nonnull
    BatchSource source;

    @Override
    public DataSource<?> build() throws DataSourceFactoryException {
        return SparkSource.builder()
                .format(source.getFormat())
                .options(source.getOptions())
                .encoder(EncoderUtils.buildEncoder(source.getEncodedAs()))
                .type(SparkSource.SourceType.BATCH)
                .build();
    }

}