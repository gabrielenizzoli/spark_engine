package dataengine.pipeline.model.description.sink;

import javax.annotation.Nonnull;
import java.util.Optional;

public interface SinkCatalog {

    @Nonnull
    Optional<Sink> lookup(String sinkName) throws SinkCatalogException;

}
