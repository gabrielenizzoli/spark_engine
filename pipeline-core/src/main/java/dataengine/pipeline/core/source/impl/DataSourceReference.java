package dataengine.pipeline.core.source.impl;


import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.core.source.DataSourceError;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Supplier;

@Value
public class DataSourceReference<T> implements DataSource<T> {

    @Nonnull
    Supplier<DataSource<T>> lookup;

    @Override
    public Dataset<T> get() {
        return Optional.ofNullable(lookup.get())
                .orElseThrow(() -> new DataSourceError("datasource not found"))
                .get();
    }

}
