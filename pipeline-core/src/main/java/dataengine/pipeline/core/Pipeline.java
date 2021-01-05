package dataengine.pipeline.core;

import dataengine.pipeline.core.consumer.DatasetConsumer;
import dataengine.pipeline.core.consumer.catalog.DatasetConsumerCatalog;
import dataengine.pipeline.core.consumer.catalog.DatasetConsumerCatalogException;
import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.pipeline.core.supplier.catalog.DatasetSupplierCatalog;
import dataengine.pipeline.core.supplier.catalog.DatasetSupplierCatalogException;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder
public class Pipeline {

    @Nonnull
    DatasetSupplierCatalog datasetSupplierCatalog;
    @Nonnull
    DatasetConsumerCatalog datasetConsumerCatalog;

    public <T> DatasetConsumer<T> run(@Nonnull String supplierName, @Nonnull String consumerName)
            throws DatasetSupplierCatalogException, DatasetConsumerCatalogException {
        DatasetSupplier<T> datasetSupplier = datasetSupplierCatalog.lookup(supplierName);
        DatasetConsumer<T> datasetConsumer = datasetConsumerCatalog.lookup(consumerName);
        return datasetSupplier.writeTo(datasetConsumer);
    }

}
