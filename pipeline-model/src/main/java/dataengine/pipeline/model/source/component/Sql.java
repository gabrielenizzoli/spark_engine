package dataengine.pipeline.model.source.component;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.encoder.DataEncoder;
import dataengine.pipeline.model.source.Component;
import dataengine.pipeline.model.source.TransformationComponentWithMultipleInputs;
import dataengine.pipeline.model.udf.UdfLibrary;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Sql.Builder.class)
public class Sql implements Component, TransformationComponentWithMultipleInputs {

    @Nonnull
    @lombok.Builder.Default
    List<String> using = List.of();
    @Nonnull
    String sql;
    @Nullable
    UdfLibrary udfs;
    @Nullable
    DataEncoder encodedAs;

}
