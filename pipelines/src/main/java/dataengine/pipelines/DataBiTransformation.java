package dataengine.pipelines;

import org.apache.spark.sql.Dataset;

import java.util.function.BiFunction;

public interface DataBiTransformation<S1, S2, D> extends BiFunction<Dataset<S1>, Dataset<S2>, Dataset<D>> {
}
