package sparkengine.plan.runtime.builder.datasetconsumer;

import sparkengine.plan.model.sink.impl.BatchSink;
import sparkengine.plan.model.sink.impl.ForeachSink;
import sparkengine.plan.model.sink.impl.StreamSink;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerException;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.Trigger;

public class WriterFormatter {

    public static <T> Stream<T> getStreamFormatter(StreamSink sink) {
        return writer -> {
            writer = writer.format(sink.getFormat()).queryName(sink.getName());
            if (sink.getOptions() != null && !sink.getOptions().isEmpty())
                writer = writer.options(sink.getOptions());
            if (sink.getCheckpointLocation() != null)
                writer = writer.option("checkpointLocation", sink.getCheckpointLocation());
            if (sink.getTrigger() != null)
                writer = writer.trigger(getStreamTrigger(sink.getTrigger()));
            if (sink.getMode() != null)
                writer = writer.outputMode(getStreamOutputMode(sink.getMode()));

            return writer;
        };
    }

    public static <T> Stream<T> getForeachFormatter(ForeachSink sink) {
        return writer -> {
            writer = writer
                    .queryName(sink.getName())
                    .trigger(getStreamTrigger(sink.getTrigger()))
                    .outputMode(getStreamOutputMode(sink.getMode()));
            if (sink.getOptions() != null && !sink.getOptions().isEmpty())
                writer = writer.options(sink.getOptions());
            return writer;
        };
    }

    public static <T> Batch<T> getBatchFormatter(BatchSink sink) {
        return writer -> {
            writer = writer.format(sink.getFormat());
            if (sink.getOptions() != null && !sink.getOptions().isEmpty())
                writer = writer.options(sink.getOptions());
            if (sink.getPartitionColumns() != null && !sink.getPartitionColumns().isEmpty())
                writer = writer.partitionBy(sink.getPartitionColumns().toArray(String[]::new));
            if (sink.getMode() != null)
                writer = writer.mode(getBatchSaveMode(sink));

            return writer;
        };
    }

    public static SaveMode getBatchSaveMode(BatchSink batchSink) throws DatasetConsumerException {
        if (batchSink.getMode() == null)
            return null;
        switch (batchSink.getMode()) {
            case APPEND:
                return SaveMode.Append;
            case IGNORE:
                return SaveMode.Ignore;
            case OVERWRITE:
                return SaveMode.Overwrite;
            case ERROR_IF_EXISTS:
                return SaveMode.ErrorIfExists;
        }
        throw new DatasetConsumerException("unmanaged batch save mode: " + batchSink.getMode());
    }

    public static Trigger getStreamTrigger(sparkengine.plan.model.sink.impl.Trigger trigger) throws DatasetConsumerException {
        if (trigger == null)
            return null;
        if (trigger instanceof sparkengine.plan.model.sink.impl.Trigger.TriggerContinuousMs)
            return Trigger.Continuous(((sparkengine.plan.model.sink.impl.Trigger.TriggerContinuousMs) trigger).getMilliseconds());
        if (trigger instanceof sparkengine.plan.model.sink.impl.Trigger.TriggerIntervalMs)
            return Trigger.ProcessingTime(((sparkengine.plan.model.sink.impl.Trigger.TriggerIntervalMs) trigger).getMilliseconds());
        if (trigger instanceof sparkengine.plan.model.sink.impl.Trigger.TriggerOnce)
            return Trigger.Once();

        throw new DatasetConsumerException("unmanaged stream trigger: " + trigger);
    }

    public static OutputMode getStreamOutputMode(StreamSink.OutputMode outputMode) throws DatasetConsumerException {
        if (outputMode == null)
            return null;
        switch (outputMode) {
            case APPEND:
                return OutputMode.Append();
            case COMPLETE:
                return OutputMode.Complete();
            case UPDATE:
                return OutputMode.Update();
        }
        throw new DatasetConsumerException("unmanaged stream output mode: " + outputMode);
    }

    public interface Stream<T> {
        DataStreamWriter<T> apply(DataStreamWriter<T> writer) throws DatasetConsumerException;
    }

    public interface Batch<T> {
        DataFrameWriter<T> apply(DataFrameWriter<T> writer) throws DatasetConsumerException;
    }

}
