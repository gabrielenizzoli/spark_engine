package dataengine.pipeline.core.supplier.factory;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.pipeline.core.supplier.impl.DatasetSupplierError;
import dataengine.pipeline.model.source.component.PlaceholderSchemaComponent;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class PlaceholderSourceFactory implements DatasetSupplierFactory {

    @Nonnull
    PlaceholderSchemaComponent source;

    @Override
    public DatasetSupplier<?> build() throws DatasetSupplierFactoryException {
        return () -> {
            throw new DatasetSupplierError("this is a placeholder for a datasource with a schema: " + source.getSchema());
        };
    }

}
