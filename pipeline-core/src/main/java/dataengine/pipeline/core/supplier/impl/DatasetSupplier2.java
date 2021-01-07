package dataengine.pipeline.core.supplier.impl;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.spark.transformation.DataTransformation2;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

/**
 * DatasetSupplier that stores information needed to perform a custom-defined operation on 2 input DatasetSuppliers
 *
 * @param <S1> type of the input DatasetSupplier #1
 * @param <S2> type of the input DatasetSupplier #2
 * @param <D>  type of the output DatasetSupplier
 */
@Value
@Builder
public class DatasetSupplier2<S1, S2, D> implements DatasetSupplier<D> {

    @Nonnull
    DatasetSupplier<S1> parentDatasetSupplier1;
    @Nonnull
    DatasetSupplier<S2> parentDatasetSupplier2;
    @Nonnull
    DataTransformation2<S1, S2, D> transformation;

    @Override
    public Dataset<D> get() {
        return transformation.apply(
                parentDatasetSupplier1.get(),
                parentDatasetSupplier2.get()
        );
    }
}
