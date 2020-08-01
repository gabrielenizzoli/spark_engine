package dataengine.pipeline.core.source.composer;

import dataengine.pipeline.core.source.DataSource;
import dataengine.pipeline.model.description.source.Component;
import dataengine.pipeline.model.description.source.TransformationComponentWithMultipleInputs;
import dataengine.pipeline.model.description.source.TransformationComponentWithSingleInput;
import lombok.*;
import lombok.experimental.FieldDefaults;
import lombok.experimental.SuperBuilder;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Value
@Builder
public class DataSourceInfo<T> {

    @Nonnull
    DataSource<T> dataSource;
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
