package sparkengine.spark.transformation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;

/**
 * Data transformation interface that performs a custom-defined operation on 2 input Datasets
 *
 * @param <S1> type of the input dataset #1
 * @param <S2> type of the input dataset #2
 * @param <D>  type of the output dataset
 */
public interface DataTransformation2<S1, S2, D> {

    /**
     * Applies the transformation.
     *
     * @param s1Dataset input dataset #1
     * @param s2Dataset input dataset #2
     * @return output dataset
     */
    Dataset<D> apply(Dataset<S1> s1Dataset, Dataset<S2> s2Dataset);

    /**
     * This utility method allows to chain a transformation on the output dataset. The transformation may maintain or change the output type.
     *
     * @param tx   transformation to apply on the output
     * @param <D2> type of the output after this transformation is applied
     * @return output dataset after the transformation
     */
    default <D2> DataTransformation2<S1, S2, D2> andThen(DataTransformation<D, D2> tx) {
        return (s1, s2) -> tx.apply(apply(s1, s2));
    }

    /**
     * This utility method applies an encoder to the output dataset.
     *
     * @param encoder encoder to apply
     * @param <D2>    type of the output after this encoding is applied
     * @return output dataset after the encoding
     */
    default <D2> DataTransformation2<S1, S2, D2> andThenEncode(Encoder<D2> encoder) {
        return andThen(Transformations.encodeAs(encoder));
    }

}
