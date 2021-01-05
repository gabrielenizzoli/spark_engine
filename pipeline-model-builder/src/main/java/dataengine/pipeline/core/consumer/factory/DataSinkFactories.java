package dataengine.pipeline.core.consumer.factory;

import dataengine.pipeline.core.consumer.impl.DatasetWriterFormat;
import dataengine.pipeline.model.description.sink.*;

import javax.annotation.Nonnull;

public class DataSinkFactories {

    public static DatasetConsumerFactory<?> factoryFor(@Nonnull Sink sink)
            throws DatasetConsumerFactoryException {
        if (sink instanceof ShowSink) {
            return new ShowFactory<>((ShowSink) sink);
        } else if (sink instanceof BatchSink) {
            return new BatchFactory<>((BatchSink) sink);
        } else if (sink instanceof CollectSink) {
            return new CollectFactory<>();
        } else if (sink instanceof StreamSink) {
            return new StreamFactory<>((StreamSink) sink);
        }
        throw new DatasetConsumerFactoryException("sink " + sink + " not managed");

    }

    public static DatasetWriterFormat getSinkFormat(SinkWithFormat sink) {
        return DatasetWriterFormat.builder()
                .format(sink.getFormat())
                .options(sink.getOptions())
                .partitionColumns(sink.getPartitionColumns())
                .build();
    }

}
