package sparkengine.spark.transformation;

import lombok.Value;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;

import javax.annotation.Nonnull;

@Value
public class DataTransformationWithMap<S, D> implements DataTransformation<S, D> {

    @Nonnull
    MapFunction<S, D> mapFunction;
    @Nonnull
    Encoder<D> encoder;

    @Override
    public Dataset<D> apply(Dataset<S> dataset) {
        return dataset.map(mapFunction, encoder);
    }

}