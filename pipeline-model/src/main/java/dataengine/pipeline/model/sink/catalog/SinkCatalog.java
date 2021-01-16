package dataengine.pipeline.model.sink.catalog;

import dataengine.pipeline.model.sink.Sink;

import javax.annotation.Nonnull;
import java.util.Optional;

public interface SinkCatalog {

    SinkCatalog EMPTY = (name) -> Optional.empty();

    @Nonnull
    Optional<Sink> lookup(String sinkName) throws SinkCatalogException;

}
