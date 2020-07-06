package dataengine.pipeline.model.builder.sink;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.sink.factory.DataSinkFactory;
import dataengine.pipeline.core.sink.impl.SinkFormat;
import dataengine.pipeline.core.sink.impl.SparkBatchSink;
import dataengine.pipeline.core.sink.impl.SparkShowSink;
import dataengine.pipeline.model.description.sink.BatchSink;
import dataengine.pipeline.model.description.sink.ShowSink;
import dataengine.pipeline.model.description.sink.Sink;
import lombok.AllArgsConstructor;

import javax.annotation.Nonnull;

@AllArgsConstructor
public class DataSinkFactoryImpl<T> implements DataSinkFactory<T> {

    @Nonnull
    Sink sink;

    @Override
    @SuppressWarnings("unchecked")
    public DataSink<T> get() {

        if (sink instanceof ShowSink) {
            ShowSink showSink = (ShowSink) sink;
            return (DataSink<T>) SparkShowSink.builder()
                    .numRows(showSink.getNumRows())
                    .truncate(showSink.getTruncate())
                    .build();
        } else if (sink instanceof BatchSink) {
            BatchSink batchSink = (BatchSink) sink;
            SinkFormat sinkFormat = SinkFormat.builder()
                    .format(batchSink.getFormat())
                    .options(batchSink.getOptions())
                    .build();
            return (DataSink<T>) SparkBatchSink.builder()
                    .format(sinkFormat)
                    .build();
        }
        return null;
        //throw new DataSourceFactoryException(sink + " not managed");
    }

}
