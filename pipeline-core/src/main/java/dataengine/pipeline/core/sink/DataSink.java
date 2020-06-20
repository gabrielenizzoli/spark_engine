package dataengine.pipeline.core.sink;

import org.apache.spark.sql.Dataset;

import java.util.function.Consumer;

public interface DataSink<T> extends Consumer<Dataset<T>> {

}
