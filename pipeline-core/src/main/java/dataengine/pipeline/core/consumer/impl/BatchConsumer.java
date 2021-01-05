package dataengine.pipeline.core.consumer.impl;

import dataengine.pipeline.core.consumer.DatasetConsumer;
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
    public void accept(Dataset<T> dataset) {
        if (dataset.isStreaming())
            throw new IllegalArgumentException("input dataset is a streaming dataset");

        var writer = format.configureBatch(dataset.write());
        Optional.ofNullable(saveMode).ifPresent(m -> writer.mode(saveMode));
        writer.save();
    }

}
