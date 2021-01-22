package sparkengine.plan.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.ComponentWithMultipleInputs;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = FragmentComponent.Builder.class)
public class FragmentComponent implements ComponentWithMultipleInputs {

    @Nullable
    List<String> using;
    @Nonnull
    String providing;
    @Nonnull
    Map<String, Component> components;

}
