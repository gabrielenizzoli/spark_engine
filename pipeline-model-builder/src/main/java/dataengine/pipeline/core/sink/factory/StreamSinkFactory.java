package dataengine.pipeline.core.sink.factory;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.sink.impl.SparkStreamSink;
import dataengine.pipeline.model.description.sink.StreamSink;
import lombok.Value;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.Trigger;

import javax.annotation.Nonnull;

@Value
public class StreamSinkFactory<T> implements DataSinkFactory<T> {

    @Nonnull
    StreamSink streamSink;

    @Override
    public DataSink<T> build() {
        return (SparkStreamSink<T>) SparkStreamSink.<Row>builder()
                .queryName(streamSink.getName())
                .checkpoint(streamSink.getCheckpointLocation())
                .format(DataSinkFactories.getSinkFormat(streamSink))
                .outputMode(getStreamOutputMode())
                .trigger(getStreamTrigger())
                .build();
    }

    private Trigger getStreamTrigger() {
        if (streamSink.getTrigger() == null)
            return null;
        if (streamSink.getTrigger() instanceof dataengine.pipeline.model.description.sink.Trigger.TriggerContinuousMs)
            return Trigger.Continuous(((dataengine.pipeline.model.description.sink.Trigger.TriggerContinuousMs)streamSink.getTrigger()).getMilliseconds());
        if (streamSink.getTrigger() instanceof dataengine.pipeline.model.description.sink.Trigger.TriggerTimeMs)
            return Trigger.ProcessingTime(((dataengine.pipeline.model.description.sink.Trigger.TriggerTimeMs)streamSink.getTrigger()).getTime());
        if (streamSink.getTrigger() instanceof dataengine.pipeline.model.description.sink.Trigger.TriggerOnce)
            return Trigger.Once();
        // TODO fix this
        return null;
    }

    private OutputMode getStreamOutputMode() {
        if (streamSink.getMode() == null)
            return null;
        switch (streamSink.getMode()) {
            case APPEND:
                return OutputMode.Append();
            case COMPLETE:
                return OutputMode.Complete();
            case UPDATE:
                return OutputMode.Update();
        }
        // TODO fix this
        return null;
    }

}
