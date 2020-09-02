package dataengine.pipeline.core.sink.factory;

import dataengine.pipeline.core.sink.impl.SinkFormat;
import dataengine.pipeline.model.description.sink.*;

import javax.annotation.Nonnull;

public class DataSinkFactories {

    public static DataSinkFactory<?> factoryFor(@Nonnull Sink sink)
            throws DataSinkFactoryException {
        if (sink instanceof ShowSink) {
            return new ShowSinkFactory<>((ShowSink) sink);
        } else if (sink instanceof BatchSink) {
            return new BatchSinkFactory<>((BatchSink) sink);
        } else if (sink instanceof CollectSink) {
            return new CollectSinkFactory<>();
        } else if (sink instanceof StreamSink) {
            return new StreamSinkFactory<>((StreamSink) sink);
        }
        throw new DataSinkFactoryException("sink " + sink + " not managed");

    }

    public static SinkFormat getSinkFormat(SinkWithFormat sink) {
        return SinkFormat.builder()
                .format(sink.getFormat())
                .options(sink.getOptions())
                .partitionColumns(sink.getPartitionColumns())
                .build();
    }

}
