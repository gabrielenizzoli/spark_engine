package dataengine.pipeline.core.supplier.catalog;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.pipeline.model.description.source.Component;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

@Value
@Builder
public class DataSourceInfo<T> {

    @Nonnull
    DatasetSupplier<T> datasetSupplier;
    @Nullable
    StructType schema;
    @Nonnull
    @Singular
    Set<String> parentDataSourceNames;
    @Nonnull
    Component component;

    public boolean isCompatible(@Nullable DataSourceInfo<?> otherDataSourceInfo) {
        if (otherDataSourceInfo == null)
            return false;
        if (schema == null)
            return true;
        if (otherDataSourceInfo.schema == null)
            return false;
        return schema.equals(otherDataSourceInfo.schema);
    }

}
