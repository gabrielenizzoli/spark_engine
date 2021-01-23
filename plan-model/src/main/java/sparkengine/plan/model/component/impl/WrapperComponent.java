package sparkengine.plan.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.ComponentWithMultipleInputs;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = WrapperComponent.Builder.class)
public class WrapperComponent implements ComponentWithMultipleInputs {

    public static final String TYPE_NAME = "wrapper";

    @Nullable
    List<String> using;
    @Nonnull
    Component component;

}

