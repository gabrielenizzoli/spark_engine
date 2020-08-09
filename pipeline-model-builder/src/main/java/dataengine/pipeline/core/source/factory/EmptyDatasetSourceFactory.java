package dataengine.pipeline.core.source.factory;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.impl.EmptyDatasetSource;
import dataengine.pipeline.core.source.utils.EncoderUtils;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class EmptyDatasetSourceFactory implements DataSourceFactory {

    @Nonnull
    dataengine.pipeline.model.description.source.component.EmptyDatasetSource source;

    @Override
    public DataSource<?> build() throws DataSourceFactoryException {
        return EmptyDatasetSource.builder()
                .encoder(EncoderUtils.buildEncoder(source.getEncodedAs()))
                .build();
    }

}
