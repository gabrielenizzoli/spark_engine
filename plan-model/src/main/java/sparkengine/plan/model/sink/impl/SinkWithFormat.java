package sparkengine.plan.model.sink.impl;

import sparkengine.plan.model.sink.Sink;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public interface SinkWithFormat extends Sink {

    @Nonnull
    String getFormat();

    @Nullable
    Map<String, String> getOptions();

}
