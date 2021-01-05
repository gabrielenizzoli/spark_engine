package dataengine.pipeline.core.supplier.impl;


import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.pipeline.core.supplier.DatasetSupplierError;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Supplier;

@Value
public class DatasetSupplierReference<T> implements DatasetSupplier<T> {

    @Nonnull
    Supplier<DatasetSupplier<T>> lookup;

    @Override
    public Dataset<T> get() {
        return Optional.ofNullable(lookup.get())
                .orElseThrow(() -> new DatasetSupplierError("DatasetSupplier not found"))
                .get();
    }

}
