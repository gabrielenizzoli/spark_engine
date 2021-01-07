package dataengine.pipeline.core.consumer.catalog;

import dataengine.pipeline.core.consumer.DatasetConsumer;

import javax.annotation.Nonnull;

@FunctionalInterface
public interface DatasetConsumerCatalog {

    @Nonnull
    <T> DatasetConsumer<T> lookup(String datasetConsumerName) throws DatasetConsumerCatalogException;

}
