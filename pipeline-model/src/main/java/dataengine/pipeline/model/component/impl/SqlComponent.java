package dataengine.pipeline.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.encoder.DataEncoder;
import dataengine.pipeline.model.component.Component;
import dataengine.pipeline.model.component.TransformationComponentWithMultipleInputs;
import dataengine.pipeline.model.udf.UdfLibrary;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = SqlComponent.Builder.class)
public class SqlComponent implements Component, TransformationComponentWithMultipleInputs {

    @Nullable
    List<String> using;
    @Nonnull
    String sql;
    @Nullable
    UdfLibrary udfs;
    @Nullable
    DataEncoder encodedAs;

}
