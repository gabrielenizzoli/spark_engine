package dataengine.pipeline.core.sink.composer;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.sink.composer.DataSinkComposer;
import dataengine.pipeline.core.sink.composer.DataSinkComposerException;
import dataengine.pipeline.core.sink.factory.*;
import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.model.description.sink.*;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.Optional;

@Value(staticConstructor = "ofCatalog")
public class DataSinkComposerImpl implements DataSinkComposer {

    @Nonnull
    SinkCatalog sinkCatalog;

    @Override
    @Nonnull
    public <T> DataSink<T> lookup(String sinkName) throws DataSinkComposerException {

        try {
            Optional<Sink> sinkOptional = sinkCatalog.lookup(sinkName);
            Sink sink = sinkOptional.orElseThrow(() -> new SinkCatalogException("can't find sink with name " + sinkName));
            DataSinkFactory<T> factory = (DataSinkFactory<T>)DataSinkFactories.factoryFor(sink);
            // TOD shouldit be always not null?
            if (factory != null) {
                return factory.build();
            }

            throw new SinkCatalogException("sink with name " + sinkName + " is of unmanaged type " + sink.getClass().getName());
        } catch (SinkCatalogException | DataSinkFactoryException e) {
            throw new IllegalArgumentException("issue with sink " + sinkName, e);
        }

    }


}
