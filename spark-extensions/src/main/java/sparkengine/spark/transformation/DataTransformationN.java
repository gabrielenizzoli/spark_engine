package sparkengine.spark.transformation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;

import java.util.Arrays;
import java.util.List;

@FunctionalInterface
public interface DataTransformationN<S, D> {

    Dataset<D> apply(List<Dataset<S>> datasets);

    default Dataset<D> apply(Dataset<S>... datasets) {
        return apply(Arrays.asList(datasets));
    }

    default <D2> DataTransformationN<S, D2> andThen(DataTransformation<D, D2> tx) {
        return datasets -> {
            Dataset<D> dataset = apply(datasets);
            return tx.apply(dataset);
        };
    }

    default <D2> DataTransformationN<S, D2> andThenEncode(Encoder<D2> encoder) {
        return andThen(Transformations.encodeAs(encoder));
    }

}
