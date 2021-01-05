package dataengine.pipeline.core.consumer.factory;

import dataengine.pipeline.core.consumer.DatasetConsumer;
import dataengine.pipeline.core.consumer.impl.CollectConsumer;

import javax.annotation.Nonnull;

public class CollectFactory<T> implements DatasetConsumerFactory<T> {

    @Nonnull
    @Override
    public DatasetConsumer<T> build() throws DatasetConsumerFactoryException {
        return CollectConsumer.<T>builder().build();
    }

}
