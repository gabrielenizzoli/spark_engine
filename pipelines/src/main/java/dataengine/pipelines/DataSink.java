package dataengine.pipelines;

import org.apache.spark.sql.Dataset;

import java.util.function.Consumer;

public interface DataSink<T> extends Consumer<Dataset<T>> {
}
