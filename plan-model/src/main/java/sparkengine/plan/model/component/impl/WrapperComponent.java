package sparkengine.plan.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.ComponentWithChild;
import sparkengine.plan.model.component.ComponentWithMultipleInputs;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = WrapperComponent.Builder.class)
public class WrapperComponent implements ComponentWithMultipleInputs, ComponentWithChild {

    public static final String TYPE_NAME = "wrapper";

    @Nullable
    List<String> using;
    @Nonnull
    @With
    Component component;

}

