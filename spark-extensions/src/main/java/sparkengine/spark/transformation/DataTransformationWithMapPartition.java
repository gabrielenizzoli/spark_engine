package sparkengine.spark.transformation;

import lombok.Value;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;

import javax.annotation.Nonnull;

@Value
public class DataTransformationWithMapPartition<S, D> implements DataTransformation<S, D> {

    @Nonnull
    MapPartitionsFunction<S, D> mapPartitionsFunction;
    @Nonnull
    Encoder<D> encoder;

    @Override
    public Dataset<D> apply(Dataset<S> dataset) {
        return dataset.mapPartitions(mapPartitionsFunction, encoder);
    }

}