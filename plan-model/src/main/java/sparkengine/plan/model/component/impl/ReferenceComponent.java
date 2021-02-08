package sparkengine.plan.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.common.Reference;
import sparkengine.plan.model.component.ComponentWithMultipleInputs;
import sparkengine.plan.model.component.ComponentWithNoRuntime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = ReferenceComponent.Builder.class)
public class ReferenceComponent implements ComponentWithMultipleInputs, ComponentWithNoRuntime, Reference {

    public static final String TYPE_NAME = "ref";

    @Nullable
    List<String> using;
    @Nonnull
    @lombok.Builder.Default
    ReferenceMode mode = ReferenceMode.RELATIVE;
    @Nullable
    String ref;

}
