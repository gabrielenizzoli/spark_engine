package dataengine.pipeline.core.supplier.factory;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.pipeline.core.supplier.impl.EmptyDatasetSource;
import dataengine.pipeline.core.supplier.utils.EncoderUtils;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class EmptyDatasetSourceFactory implements DatasetSupplierFactory {

    @Nonnull
    dataengine.pipeline.model.description.source.component.EmptyDatasetSource source;

    @Override
    public DatasetSupplier<?> build() throws DatasetSupplierFactoryException {
        return EmptyDatasetSource.builder()
                .encoder(EncoderUtils.buildEncoder(source.getEncodedAs()))
                .build();
    }

}
