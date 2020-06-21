package dataengine.pipeline.core;

import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.source.DataSource;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.apache.spark.sql.Dataset;

@Value
@Builder
public class Tap<T> implements DataSink<T>, DataSource<T> {

    @NonFinal
    Dataset<T> dataset;

    @Override
    public void accept(Dataset<T> dataset) {
        this.dataset = dataset;
    }

    @Override
    public Dataset<T> get() {
        return dataset;
    }

}
