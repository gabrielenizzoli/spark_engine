package dataengine.pipeline;

import dataengine.pipeline.transformation.Transformations;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;

public interface DataTransformation<S, D> {

    Dataset<D> apply(Dataset<S> sDataset);

    default <D2> DataTransformation<S, D2> andThen(DataTransformation<D, D2> tx) {
        return s -> tx.apply(apply(s));
    }

    default DataSink<S> andThenSink(DataSink<D> sink) {
        return s -> sink.accept(this.apply(s));
    }

    default <D2> DataTransformation<S, D2> andThenEncode(Encoder<D2> encoder) {
        return andThen(Transformations.encode(encoder));
    }

}