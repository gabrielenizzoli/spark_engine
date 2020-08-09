package dataengine.spark.transformation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;

@FunctionalInterface
public interface DataTransformation<S, D> {

    Dataset<D> apply(Dataset<S> dataset);

    default <D2> DataTransformation<S, D2> andThen(DataTransformation<D, D2> secondTransformation) {
        return s -> secondTransformation.apply(this.apply(s));
    }

    default <D2> DataTransformation<S, D2> andThenEncode(Encoder<D2> encoder) {
        return andThen(Transformations.encodeAs(encoder));
    }

}
