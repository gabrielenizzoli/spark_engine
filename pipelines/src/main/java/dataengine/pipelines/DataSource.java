package dataengine.pipelines;

import org.apache.spark.sql.Dataset;

import java.util.function.Supplier;

public interface DataSource<T> extends Supplier<Dataset<T>> {
}
