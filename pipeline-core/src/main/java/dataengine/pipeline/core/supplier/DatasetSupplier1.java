package dataengine.pipeline.core.supplier;

import dataengine.spark.transformation.DataTransformation;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

@Value
@AllArgsConstructor(staticName = "of")
@Builder
public class DatasetSupplier1<S1, D> implements DatasetSupplier<D> {

    @Nonnull
    DatasetSupplier<S1> parentDatasetSupplier1;
    @Nonnull
    DataTransformation<S1, D> transformation;

    @Override
    public Dataset<D> get() {
        return transformation.apply(parentDatasetSupplier1.get());
    }

}
