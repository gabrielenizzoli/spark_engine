package sparkengine.plan.model.sink.catalog;

import sparkengine.plan.model.sink.Sink;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;

public interface SinkCatalog {

    SinkCatalog EMPTY = (name) -> Optional.empty();

    static SinkCatalog ofMap(Map<String, Sink> sinks) {
        return SinkCatalogFromMap.of(sinks);
    }

    @Nonnull
    Optional<Sink> lookup(String sinkName) throws SinkCatalogException;

}
