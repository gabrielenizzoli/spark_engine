package dataengine.pipelines;

import dataengine.pipelines.transformations.Transformation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;

import java.util.function.Function;

public interface DataTransformation<S, D> {

    Dataset<D> apply(Dataset<S> sDataset);

    default <D2> DataTransformation<S, D2> andThenEncode(Encoder<D2> encoder) {
        return s -> Transformation.<D, D2>encode(encoder).apply(apply(s));
    }

}
