package dataengine.pipeline.model.builder.sink;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.sink.factory.DataSinkFactory;
import dataengine.pipeline.core.sink.impl.SparkShowSink;
import dataengine.pipeline.model.description.sink.ShowSink;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class ShowSinkFactory<T> implements DataSinkFactory<T> {

    @Nonnull
    ShowSink showSink;

    @Override
    public DataSink<T> build() {
        return (DataSink<T>) SparkShowSink.builder()
                .numRows(showSink.getNumRows())
                .truncate(showSink.getTruncate())
                .build();
    }

}
