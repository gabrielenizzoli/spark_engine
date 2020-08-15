package dataengine.pipeline.core.sink.impl;

import dataengine.pipeline.core.sink.DataSink;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.spark.sql.Dataset;

@EqualsAndHashCode
@ToString
public class DataSinkShow<T> implements DataSink<T> {

    @Override
    public void accept(Dataset<T> dataset) {
        dataset.show();
    }

}
