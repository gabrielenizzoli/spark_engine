package dataengine.pipeline.core.consumer.factory;

import dataengine.pipeline.core.consumer.DatasetConsumer;

import javax.annotation.Nonnull;

@FunctionalInterface
public interface DatasetConsumerFactory<T> {

    @Nonnull
    DatasetConsumer<T> build() throws DatasetConsumerFactoryException;

}
