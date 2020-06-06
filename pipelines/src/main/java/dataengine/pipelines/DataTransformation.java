package dataengine.pipelines;

import org.apache.spark.sql.Dataset;

import java.util.function.Function;

public interface DataTransformation<S, D> extends Function<Dataset<S>, Dataset<D>> {
}
