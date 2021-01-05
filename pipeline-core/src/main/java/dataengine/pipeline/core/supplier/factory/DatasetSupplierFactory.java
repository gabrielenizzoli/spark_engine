package dataengine.pipeline.core.supplier.factory;

import dataengine.pipeline.core.supplier.DatasetSupplier;

import javax.annotation.Nonnull;

@FunctionalInterface
public interface DatasetSupplierFactory<T> {

    @Nonnull
    DatasetSupplier<T> build() throws DatasetSupplierFactoryException;

}
