package dataengine.pipeline.core.sink.factory;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.sink.factory.DataSinkFactory;
import dataengine.pipeline.core.sink.impl.SinkFormat;
import dataengine.pipeline.core.sink.impl.SparkBatchSink;
import dataengine.pipeline.model.description.sink.BatchSink;
import lombok.Value;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.streaming.OutputMode;

import javax.annotation.Nonnull;

@Value
public class BatchSinkFactory<T> implements DataSinkFactory<T> {

    @Nonnull
    BatchSink batchSink;

    @Override
    public DataSink<T> build() {
        return (DataSink<T>) SparkBatchSink.builder()
                .format(DataSinkFactories.getSinkFormat(batchSink))
                .saveMode(getBatchSaveMode())
                .build();
    }

    private SaveMode getBatchSaveMode() {
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
        // TODO fix this
        return null;
    }

}
