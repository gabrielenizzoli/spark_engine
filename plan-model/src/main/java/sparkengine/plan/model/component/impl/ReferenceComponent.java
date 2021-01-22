package sparkengine.plan.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.component.ComponentWithMultipleInputs;
import sparkengine.plan.model.component.ComponentWithNoRuntime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = ReferenceComponent.Builder.class)
public class ReferenceComponent implements ComponentWithMultipleInputs, ComponentWithNoRuntime {

    public enum ReferenceType {
        RELATIVE,
        ABSOLUTE
    }

    public enum InlineMode {
        INLINE,
        WRAPPED
    }

    @Nullable
    List<String> using;
    @Nonnull
    @lombok.Builder.Default
    ReferenceType refType = ReferenceType.RELATIVE;
    @Nullable
    String ref;
    @Nonnull
    @lombok.Builder.Default
    InlineMode inlineMode = InlineMode.INLINE;

}
