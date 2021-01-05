package dataengine.pipeline.core.consumer;

import org.apache.spark.sql.Dataset;

import java.util.function.Consumer;

public interface DatasetConsumer<T> extends Consumer<Dataset<T>> {

}
