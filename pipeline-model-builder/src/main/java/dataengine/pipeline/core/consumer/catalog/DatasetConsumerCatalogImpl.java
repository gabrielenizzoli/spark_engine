package dataengine.pipeline.core.consumer.catalog;

import dataengine.pipeline.core.consumer.DatasetConsumer;
import dataengine.pipeline.core.consumer.factory.*;
import dataengine.pipeline.model.sink.Sink;
import dataengine.pipeline.model.sink.SinkCatalog;
import dataengine.pipeline.model.sink.SinkCatalogException;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.Optional;

@Value(staticConstructor = "ofCatalog")
public class DatasetConsumerCatalogImpl implements DatasetConsumerCatalog {

    @Nonnull
    SinkCatalog sinkCatalog;

    @Override
    @Nonnull
    public <T> DatasetConsumer<T> lookup(String sinkName) throws DatasetConsumerCatalogException {

        try {
            Optional<Sink> sinkOptional = sinkCatalog.lookup(sinkName);
            Sink sink = sinkOptional.orElseThrow(() -> new SinkCatalogException("can't find sink with name " + sinkName));
            DatasetConsumerFactory<T> factory = (DatasetConsumerFactory<T>)DataSinkFactories.factoryFor(sink);
            // TODO should it be always not null?
            if (factory != null) {
                return factory.build();
            }

            throw new SinkCatalogException("sink with name " + sinkName + " is of unmanaged type " + sink.getClass().getName());
        } catch (SinkCatalogException | DatasetConsumerFactoryException e) {
            throw new IllegalArgumentException("issue with sink " + sinkName, e);
        }

    }


}
