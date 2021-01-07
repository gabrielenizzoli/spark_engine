package dataengine.pipeline.datasetconsumer;

import dataengine.pipeline.datasetconsumer.utils.*;
import dataengine.pipeline.datasetfactory.ComponentDatasetFactory;
import dataengine.pipeline.model.sink.*;
import dataengine.pipeline.model.source.ComponentCatalog;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;

@Value
@Builder
public class SinkDatasetConsumerFactory {

    @Nonnull
    SinkCatalog sinkCatalog;

    public static SinkDatasetConsumerFactory of(SinkCatalog catalog) {
        return SinkDatasetConsumerFactory.builder().sinkCatalog(catalog).build();
    }

    public <T> DatasetConsumer<T> buildConsumer(String consumerName) throws DatasetConsumerException {

        if (consumerName == null || consumerName.isBlank())
            throw new DatasetConsumerException("consumer name is null or blank: [" + consumerName + "]");
        consumerName = consumerName.strip();

        var sink = getSink(consumerName);
        return getConsumer(sink);
    }

    @Nonnull
    private Sink getSink(String name) throws DatasetConsumerException {
        try {
            return sinkCatalog.lookup(name).orElseThrow(() -> new DatasetConsumerException.SinkNotFound(name));
        } catch (SinkCatalogException e) {
            throw new DatasetConsumerException("issues locating sink with name " + name, e);
        }
    }

    private <T> DatasetConsumer<T> getConsumer(Sink sink) throws DatasetConsumerException {
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

        throw new DatasetConsumerException.SinkNotManaged("sink type [" + sink.getClass().getName() + "] does not have any factory associated");

    }

}
