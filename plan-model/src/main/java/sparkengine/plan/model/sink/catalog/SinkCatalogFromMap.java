package sparkengine.plan.model.sink.catalog;

import lombok.Value;
import sparkengine.plan.model.sink.Sink;

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
