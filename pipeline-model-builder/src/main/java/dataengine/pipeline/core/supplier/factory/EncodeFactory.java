package dataengine.pipeline.core.supplier.factory;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.pipeline.core.supplier.catalog.DatasetSupplierCatalog;
import dataengine.pipeline.core.supplier.catalog.DatasetSupplierCatalogException;
import dataengine.pipeline.core.supplier.utils.EncoderUtils;
import dataengine.pipeline.model.source.component.Encode;
import lombok.Value;
import org.apache.spark.sql.Encoder;

import javax.annotation.Nonnull;

@Value
public class EncodeFactory implements DatasetSupplierFactory {

    @Nonnull
    Encode encode;
    @Nonnull
    DatasetSupplierCatalog DatasetSupplierCatalog;

    @Override
    public DatasetSupplier<?> build() throws DatasetSupplierFactoryException {
        Validate.singleInput().accept(encode);
        Encoder<?> encoder = EncoderUtils.buildEncoder(encode.getEncodedAs());
        try {
            return DatasetSupplierCatalog
                    .lookup(encode.getUsing())
                    .encodeAs(encoder);
        } catch (DatasetSupplierCatalogException e) {
            throw new DatasetSupplierFactoryException("can't locate datasource with name " + encode.getUsing(), e);
        }
    }

}
