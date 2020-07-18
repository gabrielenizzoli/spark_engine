package dataengine.pipeline.core.source.cache;

import dataengine.pipeline.core.source.DataSource;
import lombok.*;
import lombok.experimental.FieldDefaults;
import lombok.experimental.SuperBuilder;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

@Getter
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@EqualsAndHashCode
@ToString
@SuperBuilder
public class DataSourceInfo<T> {

    @Nonnull
    DataSource<T> dataSource;
    @Nullable
    StructType schema;
    @Nonnull
    @Singular
    Set<String> parentDataSourceNames;

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
