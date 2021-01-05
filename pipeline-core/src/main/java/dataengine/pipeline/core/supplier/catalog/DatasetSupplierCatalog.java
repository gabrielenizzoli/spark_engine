package dataengine.pipeline.core.supplier.catalog;

import dataengine.pipeline.core.supplier.DatasetSupplier;

import javax.annotation.Nonnull;

@FunctionalInterface
public interface DatasetSupplierCatalog {

    @Nonnull
    <T> DatasetSupplier<T> lookup(String datasetSupplierName) throws DatasetSupplierCatalogException;

}
