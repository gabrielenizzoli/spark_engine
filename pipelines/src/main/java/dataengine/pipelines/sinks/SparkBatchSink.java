package dataengine.pipelines.sinks;

import dataengine.pipelines.DataSink;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

@Value
@Builder
public class SparkBatchSink<T> implements DataSink<T> {

    @Nonnull
    SinkFormat<T> format;
    @Nullable
    SaveMode saveMode;

    @Override
    public void accept(Dataset<T> dataset) {
        if (dataset.isStreaming())
            throw new IllegalArgumentException("input dataset is a streaming dataset");

        DataFrameWriter<T> writer = format.configureBatch(dataset.write());
        Optional.ofNullable(saveMode).ifPresent(m -> writer.mode(saveMode));
        writer.save();
    }

}
