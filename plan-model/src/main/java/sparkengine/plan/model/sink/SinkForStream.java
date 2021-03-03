package sparkengine.plan.model.sink;

import sparkengine.plan.model.sink.impl.StreamSink;
import sparkengine.plan.model.sink.impl.Trigger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public interface SinkForStream extends SinkWithFormat {

    @Nonnull
    String getName();

    @Nullable
    Trigger getTrigger();

    @Nullable
    StreamSink.OutputMode getMode();

    @Nullable
    String getCheckpointLocation();

    enum OutputMode {
        APPEND,
        COMPLETE,
        UPDATE
    }

}
