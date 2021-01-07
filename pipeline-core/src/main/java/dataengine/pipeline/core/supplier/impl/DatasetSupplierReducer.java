package dataengine.pipeline.core.supplier.impl;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.spark.transformation.DataTransformation2;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Supplier;

@Value
@Builder
public class DatasetSupplierReducer<T> implements DatasetSupplier<T> {

    @Nonnull
    DatasetSupplier<T> datasetSupplier;
    @Nonnull
    @Singular
    List<DatasetSupplier<T>> parentDatasetSuppliers;
    @Nonnull
    DataTransformation2<T, T, T> reducer;

    @Override
    public Dataset<T> get() {
        return parentDatasetSuppliers.stream().map(Supplier::get).reduce(datasetSupplier.get(), reducer::apply);
    }

}
