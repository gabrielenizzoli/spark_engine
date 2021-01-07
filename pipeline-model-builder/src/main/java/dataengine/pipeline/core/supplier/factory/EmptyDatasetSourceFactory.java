package dataengine.pipeline.core.supplier.factory;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.pipeline.core.supplier.impl.EmptyDatasetSupplier;
import dataengine.pipeline.core.supplier.utils.EncoderUtils;
import dataengine.pipeline.model.source.component.EmptySource;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class EmptyDatasetSourceFactory implements DatasetSupplierFactory {

    @Nonnull
    EmptySource source;

    @Override
    public DatasetSupplier<?> build() throws DatasetSupplierFactoryException {
        return EmptyDatasetSupplier.builder()
                .encoder(EncoderUtils.buildEncoder(source.getEncodedAs()))
                .build();
    }

}
