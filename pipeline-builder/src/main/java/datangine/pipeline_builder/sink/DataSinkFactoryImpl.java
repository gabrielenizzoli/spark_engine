package datangine.pipeline_builder.sink;

import dataengine.model.pipeline.sink.Sink;
import dataengine.model.pipeline.sink.SparkBatchSink;
import dataengine.model.pipeline.sink.SparkShowSink;
import dataengine.pipeline.DataSink;
import dataengine.pipeline.sink.SinkFormat;
import datangine.pipeline_builder.PipelineBuilderException;
import datangine.pipeline_builder.validation.Validate;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class DataSinkFactoryImpl implements DataSinkFactory {

    @Override
    public DataSink apply(Sink sink) {

        Validate.sink().accept(sink);

        if (sink instanceof SparkShowSink) {
            SparkShowSink show = (SparkShowSink)sink;
            return dataengine.pipeline.sink.SparkShowSink.builder()
                    .numRows(show.getNumRows())
                    .truncate(show.getTruncate())
                    .build();
        } else if (sink instanceof SparkBatchSink) {
            SparkBatchSink sparkBatchSink = (SparkBatchSink) sink;
            SinkFormat sinkFormat = SinkFormat.builder()
                    .format(sparkBatchSink.getFormat())
                    .options(sparkBatchSink.getOptions())
                    .build();
            return dataengine.pipeline.sink.SparkBatchSink.builder()
                    .format(sinkFormat)
                    .build();
        }
        throw new PipelineBuilderException(sink + " not managed");
    }

}
