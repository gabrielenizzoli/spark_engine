package dataengine.pipeline.core.sink.factory;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.sink.factory.DataSinkFactory;
import dataengine.pipeline.core.sink.impl.SinkFormat;
import dataengine.pipeline.core.sink.impl.SparkBatchSink;
import dataengine.pipeline.model.description.sink.BatchSink;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class BatchSinkFactory<T> implements DataSinkFactory<T> {

    @Nonnull
    BatchSink batchSink;

    @Override
    public DataSink<T> build() {
        SinkFormat sinkFormat = SinkFormat.builder()
                .format(batchSink.getFormat())
                .options(batchSink.getOptions())
                .build();
        return (DataSink<T>) SparkBatchSink.builder()
                .format(sinkFormat)
                .build();
    }

}
