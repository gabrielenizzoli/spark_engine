package dataengine.pipeline.core.supplier.impl;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.spark.transformation.DataTransformationN;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Value
@AllArgsConstructor(staticName = "of")
@Builder
public class DatasetSupplierN<S, D> implements DatasetSupplier<D> {

    @Nonnull
    List<DatasetSupplier<S>> parentDatasetSuppliers;
    @Nonnull
    DataTransformationN<S, D> transformation;

    @Override
    public Dataset<D> get() {
        return transformation.apply(parentDatasetSuppliers.stream().map(Supplier::get).collect(Collectors.toList()));
    }

}
