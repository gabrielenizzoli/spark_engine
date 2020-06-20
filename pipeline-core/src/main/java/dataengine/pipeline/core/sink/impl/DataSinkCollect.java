package dataengine.pipeline.core.sink.impl;

import dataengine.pipeline.core.sink.DataSink;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.spark.sql.Dataset;

import java.util.LinkedList;
import java.util.List;

@EqualsAndHashCode
@ToString
public class DataSinkCollect<T> implements DataSink<T> {

    @Getter
    private final List<T> list = new LinkedList<>();

    @Override
    public void accept(Dataset<T> dataset) {
        list.addAll(dataset.collectAsList());
    }

}
