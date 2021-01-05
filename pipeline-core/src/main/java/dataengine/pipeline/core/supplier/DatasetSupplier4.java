package dataengine.pipeline.core.supplier;

import dataengine.spark.transformation.DataTransformation4;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

/**
 * DatasetSupplier that stores information needed to perform a custom-defined operation on 4 input DatasetSuppliers
 *
 * @param <S1> type of the input DatasetSupplier #1
 * @param <S2> type of the input DatasetSupplier #2
 * @param <S3> type of the input DatasetSupplier #3
 * @param <S4> type of the input DatasetSupplier #4
 * @param <D>  type of the output DatasetSupplier
 */
@Value
@Builder
public class DatasetSupplier4<S1, S2, S3, S4, D> implements DatasetSupplier<D> {

    @Nonnull
    DatasetSupplier<S1> parentDatasetSupplier1;
    @Nonnull
    DatasetSupplier<S2> parentDatasetSupplier2;
    @Nonnull
    DatasetSupplier<S3> parentDatasetSupplier3;
    @Nonnull
    DatasetSupplier<S4> parentDatasetSupplier4;
    @Nonnull
    DataTransformation4<S1, S2, S3, S4, D> transformation;

    @Override
    public Dataset<D> get() {
        return transformation.apply(
                parentDatasetSupplier1.get(),
                parentDatasetSupplier2.get(),
                parentDatasetSupplier3.get(),
                parentDatasetSupplier4.get()
        );
    }
}
