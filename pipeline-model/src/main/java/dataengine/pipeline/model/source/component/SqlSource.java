package dataengine.pipeline.model.source.component;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.encoder.DataEncoder;
import dataengine.pipeline.model.source.EncodedComponent;
import dataengine.pipeline.model.source.SourceComponent;
import dataengine.pipeline.model.udf.UdfLibrary;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = SqlSource.Builder.class)
@Deprecated
public class SqlSource implements SourceComponent, EncodedComponent {

    @Nonnull
    String sql;
    @Nullable
    UdfLibrary udfs;
    @Nullable
    DataEncoder encodedAs;

}
