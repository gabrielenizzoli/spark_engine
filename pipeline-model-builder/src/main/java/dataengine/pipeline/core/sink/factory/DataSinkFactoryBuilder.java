package dataengine.pipeline.core.sink.factory;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.model.description.sink.*;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.Optional;

@Value(staticConstructor = "ofCatalog")
public class DataSinkFactoryBuilder {

    @Nonnull
    SinkCatalog sinkCatalog;

    public <T> DataSink<T> lookup(String sinkName) {

        try {
            Optional<Sink> sinkOptional = sinkCatalog.lookup(sinkName);
            Sink sink = sinkOptional.orElseThrow(() -> new SinkCatalogException("can't find sink with name " + sinkName));
            DataSinkFactory<T> factory = getFactory(sink);
            if (factory != null) {
                return factory.build();
            }

            throw new SinkCatalogException("sink with name " + sinkName + " is of unmanaged type " + sink.getClass().getName());
        } catch (SinkCatalogException | DataSinkFactoryException e) {
            throw new IllegalArgumentException("issue with sink " + sinkName, e);
        }

    }

    private <T> DataSinkFactory<T> getFactory(@Nonnull Sink sink) {
        if (sink instanceof ShowSink) {
            return new ShowSinkFactory((ShowSink) sink);
        }
        if (sink instanceof CollectSink) {
            return new CollectSinkFactory();
        }
        return null;
    }

}
