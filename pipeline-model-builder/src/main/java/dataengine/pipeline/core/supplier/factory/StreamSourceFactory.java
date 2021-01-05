package dataengine.pipeline.core.supplier.factory;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.pipeline.core.supplier.impl.SparkSource;
import dataengine.pipeline.core.supplier.utils.EncoderUtils;
import dataengine.pipeline.model.description.source.component.StreamSource;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class StreamSourceFactory implements DatasetSupplierFactory {

    @Nonnull
    StreamSource source;

    @Override
    public DatasetSupplier<?> build() throws DatasetSupplierFactoryException {
        return SparkSource.builder()
                .format(source.getFormat())
                .options(source.getOptions())
                .encoder(EncoderUtils.buildEncoder(source.getEncodedAs()))
                .type(SparkSource.SourceType.STREAM)
                .build();
    }

}
