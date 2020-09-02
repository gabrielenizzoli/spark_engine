package dataengine.pipeline.model.description.sink;

import dataengine.pipeline.model.description.source.Component;
import dataengine.pipeline.model.description.source.ComponentCatalog;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;

@Value(staticConstructor = "of")
public class SinkCatalogFromMap implements SinkCatalog {

    @Nonnull
    Map<String, Sink> sinks;

    @Nonnull
    @Override
    public Optional<Sink> lookup(String sinkName) {
        return Optional.ofNullable(sinks.get(sinkName));
    }

}
