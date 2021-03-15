package sparkengine.plan.model.udf;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Value;

import javax.annotation.Nonnull;

@Value
@Builder(setterPrefix = "with")
@JsonDeserialize(builder = UdfWithScalaScript.Builder.class)
public class UdfWithScalaScript implements Udf {

    @Nonnull
    String name;
    @Nonnull
    String scala;

}
