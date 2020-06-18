package dataengine.pipelines;

import dataengine.pipelines.transformations.Transformations;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;

public interface Data2Transformation<S1, S2, D> {

    Dataset<D> apply(Dataset<S1> s1Dataset, Dataset<S2> s2Dataset);

    default <D2> Data2Transformation<S1, S2, D2> andThen(DataTransformation<D, D2> tx) {
        return (s1, s2) -> tx.apply(apply(s1, s2));
    }

    default <D2> Data2Transformation<S1, S2, D2> andThenEncode(Encoder<D2> encoder) {
        return andThen(Transformations.encode(encoder));
    }

}
