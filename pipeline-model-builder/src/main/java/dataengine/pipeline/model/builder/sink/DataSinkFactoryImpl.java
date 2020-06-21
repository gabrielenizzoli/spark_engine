package dataengine.pipeline.model.builder.sink;

import dataengine.pipeline.model.builder.Validate;
import dataengine.pipeline.model.pipeline.sink.Sink;
import dataengine.pipeline.model.pipeline.sink.SparkBatchSink;
import dataengine.pipeline.model.pipeline.sink.SparkShowSink;
import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.sink.factory.DataSinkFactory;
import dataengine.pipeline.core.sink.impl.SinkFormat;
import dataengine.pipeline.core.DataFactoryException;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.Map;

@Value
@Builder
public class DataSinkFactoryImpl implements DataSinkFactory {

    @Nonnull
    @Singular
    Map<String, Sink> sinks;

    @Override
    public DataSink apply(String name) {

        Sink sink = sinks.get(name);
        Validate.sink().accept(sink);

        if (sink instanceof SparkShowSink) {
            SparkShowSink show = (SparkShowSink)sink;
            return dataengine.pipeline.core.sink.impl.SparkShowSink.builder()
                    .numRows(show.getNumRows())
                    .truncate(show.getTruncate())
                    .build();
        } else if (sink instanceof SparkBatchSink) {
            SparkBatchSink sparkBatchSink = (SparkBatchSink) sink;
            SinkFormat sinkFormat = SinkFormat.builder()
                    .format(sparkBatchSink.getFormat())
                    .options(sparkBatchSink.getOptions())
                    .build();
            return dataengine.pipeline.core.sink.impl.SparkBatchSink.builder()
                    .format(sinkFormat)
                    .build();
        }
        throw new DataFactoryException(sink + " not managed");
    }

}
