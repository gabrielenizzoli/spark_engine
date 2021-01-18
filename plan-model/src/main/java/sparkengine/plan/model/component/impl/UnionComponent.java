package sparkengine.plan.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.ComponentWithMultipleInputs;

import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = UnionComponent.Builder.class)
public class UnionComponent implements Component, ComponentWithMultipleInputs {

    @Nullable
    List<String> using;

}
