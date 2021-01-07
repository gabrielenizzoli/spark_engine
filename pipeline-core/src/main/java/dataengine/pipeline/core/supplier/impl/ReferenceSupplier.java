package dataengine.pipeline.core.supplier.impl;


import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.pipeline.core.supplier.factory.DatasetSupplierFactory;
import dataengine.pipeline.core.supplier.factory.DatasetSupplierFactoryException;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

@Value
public class ReferenceSupplier<T> implements DatasetSupplier<T> {

    @Nonnull
    DatasetSupplierFactory<T> supplierFactory;

    @Override
    public Dataset<T> get() {
        return getDatasetSupplier().get();
    }

    @Nonnull
    private DatasetSupplier<T> getDatasetSupplier() {
        try {
            return supplierFactory.build();
        } catch (DatasetSupplierFactoryException e) {
            throw new DatasetSupplierError("DatasetSupplier not found", e);
        }
    }

}
