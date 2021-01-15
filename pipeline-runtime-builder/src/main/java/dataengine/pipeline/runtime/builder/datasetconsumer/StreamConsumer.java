package dataengine.pipeline.runtime.builder.datasetconsumer;

import dataengine.pipeline.model.sink.impl.StreamSink;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumer;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerException;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactoryException;
import lombok.Builder;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.Trigger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

@Value
@Builder
public class StreamConsumer<T> implements DatasetConsumer<T> {

    @Nonnull
    DatasetWriterFormat format;
    @Nonnull
    String queryName;
    @Nullable
    String checkpoint;
    @Nullable
    Trigger trigger;
    @Nullable
    OutputMode outputMode;

    @Override
    public DatasetConsumer<T> readFrom(Dataset<T> dataset) throws DatasetConsumerException {
        if (!dataset.isStreaming())
            throw new DatasetConsumerException("input dataset is not a streaming dataset");

        DataStreamWriter<?> writer = format.configureStream(dataset.writeStream()).queryName(queryName);
        Optional.ofNullable(trigger).ifPresent(o -> writer.trigger(trigger));
        Optional.ofNullable(outputMode).ifPresent(o -> writer.outputMode(outputMode));
        Optional.ofNullable(checkpoint).filter(StringUtils::isNotBlank).map(String::trim).ifPresent(o -> writer.option("checkpointLocation", checkpoint));
        try {
            writer.start();
        } catch (TimeoutException e) {
            throw new DatasetConsumerException("error starting stream", e);
        }

        return this;
    }

    public static Trigger getStreamTrigger(StreamSink streamSink) throws DatasetConsumerFactoryException {
        if (streamSink.getTrigger() == null)
            return null;
        if (streamSink.getTrigger() instanceof dataengine.pipeline.model.sink.impl.Trigger.TriggerContinuousMs)
            return Trigger.Continuous(((dataengine.pipeline.model.sink.impl.Trigger.TriggerContinuousMs) streamSink.getTrigger()).getMilliseconds());
        if (streamSink.getTrigger() instanceof dataengine.pipeline.model.sink.impl.Trigger.TriggerIntervalMs)
            return Trigger.ProcessingTime(((dataengine.pipeline.model.sink.impl.Trigger.TriggerIntervalMs) streamSink.getTrigger()).getMilliseconds());
        if (streamSink.getTrigger() instanceof dataengine.pipeline.model.sink.impl.Trigger.TriggerOnce)
            return Trigger.Once();
        throw new DatasetConsumerFactoryException.UnmanagedParameter("unmanaged stream trigger: " + streamSink.getTrigger());
    }

    public static OutputMode getStreamOutputMode(StreamSink streamSink) throws DatasetConsumerFactoryException {
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
        throw new DatasetConsumerFactoryException.UnmanagedParameter("unmanaged stream output mode: " + streamSink.getTrigger());
    }

}
