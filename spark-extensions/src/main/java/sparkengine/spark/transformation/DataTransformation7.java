package sparkengine.spark.transformation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;

/**
 * Data transformation interface that performs a custom-defined operation on 3 input Datasets
 *
 * @param <S1> type of the input dataset #1
 * @param <S2> type of the input dataset #2
 * @param <S3> type of the input dataset #3
 * @param <S4> type of the input dataset #4
 * @param <S5> type of the input dataset #5
 * @param <S6> type of the input dataset #6
 * @param <S7> type of the input dataset #7
 * @param <D>  type of the output dataset
 */
public interface DataTransformation7<S1, S2, S3, S4, S5, S6, S7, D> {

    /**
     * Applies the transformation.
     *
     * @param s1Dataset input dataset #1
     * @param s2Dataset input dataset #2
     * @param s3Dataset input dataset #3
     * @param s4Dataset input dataset #4
     * @param s5Dataset input dataset #5
     * @param s6Dataset input dataset #6
     * @param s7Dataset input dataset #7
     * @return output dataset
     */
    Dataset<D> apply(
            Dataset<S1> s1Dataset,
            Dataset<S2> s2Dataset,
            Dataset<S3> s3Dataset,
            Dataset<S4> s4Dataset,
            Dataset<S5> s5Dataset,
            Dataset<S6> s6Dataset,
            Dataset<S7> s7Dataset
    );

    /**
     * This utility method allows to chain a transformation on the output dataset. The transformation may maintain or change the output type.
     *
     * @param tx   transformation to apply on the output
     * @param <D2> type of the output after this transformation is applied
     * @return output dataset after the transformation
     */
    default <D2> DataTransformation7<S1, S2, S3, S4, S5, S6, S7, D2> andThen(DataTransformation<D, D2> tx) {
        return (s1, s2, s3, s4, s5, s6, s7) -> tx.apply(apply(s1, s2, s3, s4, s5, s6, s7));
    }

    /**
     * This utility method applies an encoder to the output dataset.
     *
     * @param encoder encoder to apply
     * @param <D2>    type of the output after this encoding is applied
     * @return output dataset after the encoding
     */
    default <D2> DataTransformation7<S1, S2, S3, S4, S5, S6, S7, D2> andThenEncode(Encoder<D2> encoder) {
        return andThen(Transformations.encodeAs(encoder));
    }

}
