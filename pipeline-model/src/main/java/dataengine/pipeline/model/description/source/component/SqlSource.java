package dataengine.pipeline.model.description.source.component;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.description.encoder.DataEncoder;
import dataengine.pipeline.model.description.source.EncodedComponent;
import dataengine.pipeline.model.description.source.SourceComponent;
import dataengine.pipeline.model.description.udf.UdfLibrary;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = SqlSource.Builder.class)
public class SqlSource implements SourceComponent, EncodedComponent {

    @Nonnull
    String sql;
    @Nullable
    UdfLibrary udfs;
    @Nullable
    DataEncoder encodedAs;

}
