package dataengine.pipeline.core.consumer.factory;

import dataengine.pipeline.core.consumer.DatasetConsumer;
import dataengine.pipeline.core.consumer.impl.ShowConsumer;
import dataengine.pipeline.model.sink.ShowSink;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
public class ShowFactory<T> implements DatasetConsumerFactory<T> {

    @Nonnull
    ShowSink showSink;

    @Nonnull
    @Override
    public DatasetConsumer<T> build() {
        return (DatasetConsumer<T>) ShowConsumer.builder()
                .numRows(showSink.getNumRows())
                .truncate(showSink.getTruncate())
                .build();
    }

}
