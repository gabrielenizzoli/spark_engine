package dataengine.pipelines;

import dataengine.pipelines.sql.SparkSqlUnresolvedRelationResolver;
import dataengine.pipelines.transformations.Transformation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.annotation.Nonnull;
import java.util.function.BiFunction;

public interface DataBiTransformation<S1, S2, D> {

    Dataset<D> apply(Dataset<S1> s1Dataset, Dataset<S2> s2Dataset);

    default <D2> DataBiTransformation<S1, S2, D2> andThenEncode(Encoder<D2> encoder) {
        return (s1, s2) -> Transformation.<D, D2>encode(encoder).apply(apply(s1, s2));
    }

}
