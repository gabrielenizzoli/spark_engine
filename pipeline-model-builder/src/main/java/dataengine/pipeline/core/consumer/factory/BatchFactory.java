package dataengine.pipeline.core.consumer.factory;

import dataengine.pipeline.core.consumer.DatasetConsumer;
import dataengine.pipeline.core.consumer.impl.BatchConsumer;
import dataengine.pipeline.model.sink.BatchSink;
import lombok.Value;
import org.apache.spark.sql.SaveMode;

import javax.annotation.Nonnull;

@Value
public class BatchFactory<T> implements DatasetConsumerFactory<T> {

    @Nonnull
    BatchSink batchSink;

    @Nonnull
    @Override
    public DatasetConsumer<T> build() {
        return (DatasetConsumer<T>) BatchConsumer.builder()
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
