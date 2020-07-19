package dataengine.pipeline.core.source.composer;

import dataengine.pipeline.model.description.source.Component;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import lombok.experimental.SuperBuilder;

import javax.annotation.Nonnull;

@Getter
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@SuperBuilder
public class DataSourceInfoWithComponent<T> extends DataSourceInfo<T> {

    @Nonnull
    Component component;

}
