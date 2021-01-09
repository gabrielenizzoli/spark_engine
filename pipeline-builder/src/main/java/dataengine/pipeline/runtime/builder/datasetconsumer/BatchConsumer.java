package dataengine.pipeline.runtime.builder.datasetconsumer;

import dataengine.pipeline.model.sink.impl.BatchSink;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

@Value
@Builder
public class BatchConsumer<T> implements DatasetConsumer<T> {

    @Nonnull
    DatasetWriterFormat format;
    @Nullable
    SaveMode saveMode;

    @Override
    public DatasetConsumer<T> readFrom(Dataset<T> dataset) {
        if (dataset.isStreaming())
            throw new IllegalArgumentException("input dataset is a streaming dataset");

        var writer = format.configureBatch(dataset.write());
        Optional.ofNullable(saveMode).ifPresent(m -> writer.mode(saveMode));
        writer.save();
        return this;
    }

    public static SaveMode getBatchSaveMode(BatchSink batchSink) {
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
