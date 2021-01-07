package dataengine.pipeline.core.supplier.factory;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.pipeline.core.supplier.impl.SourceSupplier;
import dataengine.pipeline.core.supplier.utils.EncoderUtils;
import dataengine.pipeline.model.source.component.StreamSource;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class StreamSourceFactory implements DatasetSupplierFactory {

    @Nonnull
    StreamSource source;

    @Override
    public DatasetSupplier<?> build() throws DatasetSupplierFactoryException {
        return SourceSupplier.builder()
                .format(source.getFormat())
                .options(source.getOptions())
                .encoder(EncoderUtils.buildEncoder(source.getEncodedAs()))
                .type(SourceSupplier.SourceType.STREAM)
                .build();
    }

}
