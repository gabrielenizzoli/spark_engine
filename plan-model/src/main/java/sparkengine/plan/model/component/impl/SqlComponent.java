package sparkengine.plan.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.ComponentWithMultipleInputs;
import sparkengine.plan.model.encoder.DataEncoder;
import sparkengine.plan.model.udf.UdfLibrary;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = SqlComponent.Builder.class)
public class SqlComponent implements ComponentWithMultipleInputs {

    public static final String TYPE_NAME = "sql";

    @Nullable
    List<String> using;
    @Nonnull
    String sql;
    @Nullable
    UdfLibrary udfs;
    @Nullable
    DataEncoder encodedAs;

}
