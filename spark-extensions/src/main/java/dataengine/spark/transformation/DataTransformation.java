package dataengine.spark.transformation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;

public interface DataTransformation<S, D> {

    Dataset<D> apply(Dataset<S> dataset);

    default <D2> DataTransformation<S, D2> andThen(DataTransformation<D, D2> tx) {
        return s -> tx.apply(apply(s));
    }

    default <D2> DataTransformation<S, D2> andThenEncode(Encoder<D2> encoder) {
        return andThen(Transformations.encodeAs(encoder));
    }

}
