package dataengine.pipeline.model.sink.impl;

import dataengine.pipeline.model.sink.Sink;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public interface SinkWithFormat extends Sink {

    @Nonnull
    String getFormat();

    @Nullable
    Map<String, String> getOptions();

}
