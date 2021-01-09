package dataengine.pipeline.runtime.builder.datasetconsumer;

import dataengine.pipeline.model.sink.Sink;
import dataengine.pipeline.model.sink.catalog.SinkCatalog;
import dataengine.pipeline.model.sink.catalog.SinkCatalogException;
import dataengine.pipeline.model.sink.impl.BatchSink;
import dataengine.pipeline.model.sink.impl.CollectSink;
import dataengine.pipeline.model.sink.impl.ShowSink;
import dataengine.pipeline.model.sink.impl.StreamSink;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumer;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactory;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactoryException;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;

@Value
@Builder
public class SinkDatasetConsumerFactory implements DatasetConsumerFactory {

    @Nonnull
    SinkCatalog sinkCatalog;

    public static SinkDatasetConsumerFactory of(SinkCatalog catalog) {
        return SinkDatasetConsumerFactory.builder().sinkCatalog(catalog).build();
    }

    @Override
    public <T> DatasetConsumer<T> buildConsumer(String consumerName) throws DatasetConsumerFactoryException {

        if (consumerName == null || consumerName.isBlank())
            throw new DatasetConsumerFactoryException("consumer name is null or blank: [" + consumerName + "]");
        consumerName = consumerName.strip();

        var sink = getSink(consumerName);
        return getConsumer(sink);
    }

    @Nonnull
    private Sink getSink(String name) throws DatasetConsumerFactoryException {
        try {
            return sinkCatalog.lookup(name).orElseThrow(() -> new DatasetConsumerFactoryException.ConsumerNotFound(name));
        } catch (SinkCatalogException e) {
            throw new DatasetConsumerFactoryException("issues locating sink with name " + name, e);
        }
    }

    private <T> DatasetConsumer<T> getConsumer(Sink sink) throws DatasetConsumerFactoryException {
        if (sink instanceof ShowSink) {
            var show = (ShowSink) sink;
            return (DatasetConsumer<T>)ShowConsumer.builder()
                    .count(show.getNumRows()).truncate(show.getTruncate())
                    .build();
        }

        if (sink instanceof CollectSink) {
            var collect = (CollectSink) sink;
            return new CollectConsumer<>(collect.getLimit());
        }

        if (sink instanceof BatchSink) {
            var batch = (BatchSink) sink;
            return (DatasetConsumer<T>) BatchConsumer.builder()
                    .format(DatasetWriterFormat.getSinkFormat(batch))
                    .saveMode(BatchConsumer.getBatchSaveMode(batch))
                    .build();
        }

        if (sink instanceof StreamSink) {
            var stream = (StreamSink) sink;
            return (StreamConsumer<T>) StreamConsumer.<Row>builder()
                    .queryName(stream.getName())
                    .checkpoint(stream.getCheckpointLocation())
                    .format(DatasetWriterFormat.getSinkFormat(stream))
                    .outputMode(StreamConsumer.getStreamOutputMode(stream))
                    .trigger(StreamConsumer.getStreamTrigger(stream))
                    .build();
        }

        throw new DatasetConsumerFactoryException.ConsumerInstantiationException("sink type [" + sink.getClass().getName() + "] does not have any factory associated");

    }

}
