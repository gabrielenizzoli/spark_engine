package sparkengine.plan.model.component.impl;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;
import sparkengine.plan.model.component.ComponentWithMultipleInputs;

import javax.annotation.Nullable;
import java.util.List;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = UnionComponent.UnionComponentBuilder.class)
public class UnionComponent implements ComponentWithMultipleInputs {

    public static final String TYPE_NAME = "union";

    @Nullable
    List<String> using;

}
