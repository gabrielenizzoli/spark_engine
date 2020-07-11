package dataengine.pipeline.model.description.source.component;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dataengine.pipeline.model.description.source.TransformationComponentWithMultipleInputs;
import dataengine.pipeline.model.description.source.Component;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = Sql.Builder.class)
public class Sql implements Component, TransformationComponentWithMultipleInputs {

    @Nonnull
    List<String> using;
    @Nonnull
    String sql;

}
